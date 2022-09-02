"""
python3

Rebuild OSM features from OSM urls

OSM features are identified as either relations, ways, or nodes (OSM directions can also be parsed)

"""

import sys
import os
import json
import configparser
import itertools
from pathlib import Path

import pandas as pd
import shapely.wkt

from prefect import flow
from prefect.task_runners import SequentialTaskRunner, ConcurrentTaskRunner
from prefect_dask.task_runners import DaskTaskRunner

import utils

import importlib
importlib.reload(utils)


# ensure correct working directory when running as a batch parallel job
# in all other cases user should already be in project directory
if not hasattr(sys, 'ps1'):
    os.chdir(os.path.dirname(__file__))

# read config file
config = configparser.ConfigParser()
config.read('config.ini')

base_dir = config["main"]["base_directory"]
run_name = config["main"]["active_run_name"]

github_name = config["main"]["github_name"]
github_repo = config["main"]["github_repo"]
github_branch = config["main"]["github_branch"]



release_name = config[run_name]["release_name"]

sample_size = int(config[run_name]["sample_size"])

# fields from input csv
id_field = config[run_name]["id_field"]
location_field = config[run_name]["location_field"]
precision_field = config[run_name]["precision_field"]

# search string used to identify relevant OSM link within the location_field of input csv
osm_str = config[run_name]["osm_str"]
invalid_str_list = json.loads(config[run_name]["invalid_str_list"])

output_project_fields = json.loads(config[run_name]["output_project_fields"])


prepare_only = config.getboolean(run_name, "prepare_only")

use_existing_svg  = config.getboolean(run_name, "use_existing_svg")
use_existing_feature  = config.getboolean(run_name, "use_existing_feature")
from_existing = use_existing_svg or use_existing_feature

if from_existing:
    existing_timestamp = config[run_name]["existing_timestamp"]

# update_mode = config.getboolean(run_name, "update_mode")
# update_ids = None
# if update_mode:
#     update_ids = json.loads(config[run_name]["update_ids"])
#     update_timestamp = config[run_name]["update_timestamp"]


prefect_cloud_enabled = config.getboolean("main", "prefect_cloud_enabled")
prefect_project_name = config["main"]["prefect_project_name"]

dask_enabled = config.getboolean("main", "dask_enabled")
dask_distributed = config.getboolean("main", "dask_distributed") if "dask_distributed" in config["main"] else False

non_dask_serial = config.getboolean("main", "non_dask_serial")

max_workers = int(config["main"]["max_workers"])


if dask_enabled:
    if dask_distributed:
        dask_address = config["main"]["dask_address"]
        ActiveTaskRunner = DaskTaskRunner(address=dask_address)

    else:
        ActiveTaskRunner = DaskTaskRunner()

else:
    if non_dask_serial:
        ActiveTaskRunner = SequentialTaskRunner()
    else:
        ActiveTaskRunner = ConcurrentTaskRunner()


# ==========================================================


timestamp = utils.get_current_timestamp('%Y_%m_%d_%H_%M')

base_dir = Path(base_dir)

# directory where all outputs will be saved
output_dir = base_dir / "output_data" / release_name / "results" / timestamp

utils.init_output_dir(output_dir)

feature_prep_df_path = output_dir / "feature_prep.csv"
task_results_path = output_dir / "task_results.csv"
processing_valid_path = output_dir / "processing_valid.csv"
processing_errors_path = output_dir / "processing_errors.csv"


api = utils.init_overpass_api()

# input_data_df = utils.load_input_data(base_dir, release_name, output_project_fields, id_field, location_field)
input_data_df = utils.load_simple_input_data(base_dir, release_name, output_project_fields, id_field, location_field, precision_field)

base_df = input_data_df[['id', 'location', 'version', 'precision']].copy()

link_df = utils.get_osm_links(base_df, osm_str, invalid_str_list, output_dir=output_dir)



if from_existing:
    existing_dir = output_dir.parent / existing_timestamp

    # TODO: determine if we actually want to use update_mode and update/integrate into function if we do
    link_df = utils.load_existing(existing_dir, link_df, use_existing_feature)


# option to sample data for testing; sample size <=0 returns full dataset
sampled_feature_prep_df = utils.sample_and_validate(link_df, sample_size=-1, summary=True)

# TODO: deduplicate svg links before processing
# TODO: optimize webdriver (0.2gb per process w/ GUI vs 2.5gb headless) [70 tasks run in a bout 5 minutes with 10 processes / GUI]
feature_prep_df = utils.generate_svg_paths(sampled_feature_prep_df, overwrite=False, upper_limit=None, nprocs=max_workers)

utils.save_df(feature_prep_df, feature_prep_df_path)

if prepare_only:
    sys.exit("Completed preparing feature_prep_df.csv, and exiting as `prepare_only` option was set.")



# ==========================================================
# process osm links into raw feature data

print("Running feature generation")
# get_osm_feat for each row in feature_prep_df
#     - parallelize
#     - buffer lines/points to create polygons
#     - convert all features to multipolygons


def generate_task_list(df, api):
    # generate list of tasks to iterate over
    task_list = list(map(list, zip(
        df["unique_id"],
        df["clean_link"],
        df["osm_type"],
        df["osm_id"],
        df["svg_path"],
        itertools.repeat(api),
        df["version"]
    )))
    return task_list


# only generate tasks for rows that have not been processed yet (checking field from potential existing data)
task_df = feature_prep_df.loc[feature_prep_df.feature.isnull() & feature_prep_df.flag.isnull()].copy()
task_list = generate_task_list(task_df, api)


# BOTTLENECK
# TODO: optimize parallelization [~1500 mixed type tasks about 45 minutes to run w/ 10 workers]

# prefect
@flow(task_runner=ActiveTaskRunner)
def osm_features_flow4():
    task_futures = utils.get_osm_feat.map(task_list[:5])
    for future in task_futures:
        future.wait()
    # utils.process(task_results, task_list, task_results_path)


osm_features_flow4()

results_df = pd.read_csv(task_results_path)


# ==========================================================
# join processing results with existing data and validate results

output_df = feature_prep_df.merge(results_df, on="unique_id", how="left")
output_df['feature'] = output_df['feature_x'].where(output_df['feature_x'].notnull(), output_df['feature_y'])
output_df['flag'] = output_df['flag_x'].where(output_df['flag_x'].notnull(), output_df['flag_y'])
output_df.drop(columns=['feature_x', 'feature_y', 'flag_x', 'flag_y'], inplace=True)

valid_df = output_df[output_df.feature.notnull() & output_df.flag.isnull()].copy()
unprocessed_df = output_df[output_df.feature.isnull() & output_df.flag.isnull()].copy()
errors_df = output_df[output_df.feature.isnull() & output_df.flag.notnull()].copy()

print("\t{} errors found out of {} tasks ({} were not procesed)".format(len(errors_df), len(output_df), len(unprocessed_df)))

utils.save_df(valid_df, processing_valid_path)
utils.save_df(errors_df, processing_errors_path)




# ==========================================================
# turn raw feature data into fully formed features and geojsons

print("Building GeoJSONs")

# valid_df = pd.read_csv(processing_valid_path)

# features from existing data need to be loaded from wkt
valid_df['feature'] = valid_df['feature'].apply(lambda x: shapely.wkt.loads(x) if isinstance(x, str) else x)

grouped_df = utils.prepare_multipolygons(valid_df)

grouped_df["geojson_path"] = grouped_df.id.apply(lambda x: output_dir / "geojsons" / f"{x}.geojson")

# join original project fields back to be included in geojson properties
grouped_df = grouped_df.merge(input_data_df, on='id', how="left")

# create individual geojsons
for ix, row in grouped_df.iterrows():
    path, geom, props = utils.prepare_single_feature(row)
    utils.output_single_feature_geojson(geom, props, path)


# ==========================================================
# combine features into final dataset


# create combined GeoJSON for all data
# combined_gdf = utils.load_all_geojsons(output_dir)
combined_gdf = utils.load_project_geojsons(output_dir, grouped_df['id'].to_list())

# add github geojson urls
combined_gdf["viz_geojson_url"] = combined_gdf.id.apply(lambda x: f"https://github.com/{github_name}/{github_repo}/blob/{github_branch}/latest/geojsons/{x}.geojson")
combined_gdf["dl_geojson_url"] = combined_gdf.id.apply(lambda x: f"https://raw.githubusercontent.com/{github_name}/{github_repo}/{github_branch}/latest/geojsons/{x}.geojson")

utils.export_combined_data(combined_gdf, output_dir)


# final summary output
print(f"""
Dataset complete: {timestamp}
\t{output_dir}
To set this as latest dataset: \n\t bash {base_dir}/set_latest.sh {release_name} {timestamp}
""")
