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
import time
import shutil

import shapely.wkt

from prefect import flow
from prefect.task_runners import SequentialTaskRunner, ConcurrentTaskRunner
from prefect_dask.task_runners import DaskTaskRunner


config = configparser.ConfigParser()

# make sure config file exists
if not os.path.exists('config.ini'):
    raise Exception('No config.ini file found')

config.read('config.ini')

# confirm config file is for this project
if 'china-osm-geodata' not in config['main']:
    raise Exception('Config.ini does not seem to be for china-osm-geodata')

# make sure we are in the correct directory
base_dir = config["main"]["base_directory"]
os.chdir(base_dir)

import utils

# import importlib
# importlib.reload(utils)


run_name = config["main"]["active_run_name"]

github_name = config["main"]["github_name"]
github_repo = config["main"]["github_repo"]
github_branch = config["main"]["github_branch"]

release_name = config[run_name]["release_name"]
csv_name = config[run_name]["csv_name"]

sample_size = int(config[run_name]["sample_size"])

# fields from input csv
id_field = config[run_name]["id_field"]
location_field = config[run_name]["location_field"]
precision_field = config[run_name]["precision_field"]

# search string used to identify relevant OSM link within the location_field of input csv
osm_str = config[run_name]["osm_str"]
invalid_str_list = json.loads(config[run_name]["invalid_str_list"])

# output_project_fields = json.loads(config[run_name]["output_project_fields"])
individual_project_fields = json.loads(config[run_name]["individual_project_fields"])
group_project_fields = json.loads(config[run_name]["group_project_fields"])
output_project_fields = list(set(individual_project_fields + group_project_fields))

prepare_only = config.getboolean(run_name, "prepare_only")

use_existing_svg  = config.getboolean(run_name, "use_existing_svg")
use_existing_feature  = config.getboolean(run_name, "use_existing_feature")
use_existing_raw_osm  = config.getboolean(run_name, "use_existing_raw_osm")
from_existing = use_existing_svg or use_existing_feature
use_only_existing  = config.getboolean(run_name, "use_only_existing")
build_missing_cache  = config.getboolean(run_name, "build_missing_cache")

if from_existing:
    existing_timestamp = config[run_name]["existing_timestamp"]

# update_mode = config.getboolean(run_name, "update_mode")
# update_ids = None
# if update_mode:
#     update_ids = json.loads(config[run_name]["update_ids"])
#     update_timestamp = config[run_name]["update_timestamp"]


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
input_data_df = utils.load_simple_input_data(base_dir, release_name, csv_name, output_project_fields, id_field, location_field, precision_field)

base_df = input_data_df[['id', 'location', 'version', 'precision']].copy()

link_df = utils.get_osm_links(base_df, osm_str, invalid_str_list, output_dir=output_dir)



if from_existing:
    existing_dir = output_dir.parent / existing_timestamp

    # TODO: determine if we actually want to use update_mode and update/integrate into function if we do
    link_df = utils.load_existing(existing_dir, link_df, use_existing_feature, use_only_existing)


# option to sample data for testing; sample size <=0 returns full dataset
sampled_feature_prep_df = utils.sample_and_validate(link_df, sample_size=sample_size, summary=True)

# TODO: deduplicate svg links before processing
svg_overwrite = not use_existing_svg
svg_feature_prep_df = utils.generate_svg_paths(sampled_feature_prep_df, overwrite=svg_overwrite, upper_limit=None, nprocs=max_workers)

utils.save_df(svg_feature_prep_df, feature_prep_df_path)


# print out svg gen errors and create separate df without errors?
feature_prep_df = svg_feature_prep_df[svg_feature_prep_df['svg_path'] != "error"].copy()

if prepare_only:
    print(f'Dataset prep complete: {timestamp}')
    sys.exit("Exiting as `prepare_only` option was set.")


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

if use_existing_raw_osm:
    shutil.copytree(existing_dir / "osm_geojsons" / "cache", output_dir / "osm_geojsons" / "cache", dirs_exist_ok=True)


if build_missing_cache:
    # generate tasks for rows that may have a feature but the cached osm data is missing
    feature_prep_df["missing_cache"] = feature_prep_df["osm_id"].apply(lambda x: not os.path.exists(output_dir / "osm_geojsons" / "cache" / f"{x}.geojson"))
    task_df = feature_prep_df.loc[feature_prep_df.flag.isnull() & (feature_prep_df.feature.isnull() | (feature_prep_df.missing_cache & (feature_prep_df.osm_type != "directions")))].copy()
else:
    # only generate tasks for rows that have not been processed yet (checking field from potential existing data)
    task_df = feature_prep_df.loc[feature_prep_df.feature.isnull() & feature_prep_df.flag.isnull()].copy()

# subset for testing if needed
active_task_df = task_df.iloc[:].copy()

task_list = generate_task_list(active_task_df, api)

# reduce task list to avoid running duplicate tasks
unique_links = []
unique_task_list = []
for i in task_list:
    if i[1] not in unique_links:
        unique_links.append(i[1])
        unique_task_list.append(i)


# prefect
@flow(task_runner=ActiveTaskRunner, persist_result=True)
def osm_features_flow(flow_task_list, overwrite=False):
    task_results = []
    for i in flow_task_list:
        osm_id = i[3]
        cache_path = output_dir / "osm_geojsons"/ "cache" / f"{osm_id}.geojson"
        if i[2] != "directions" and not overwrite and os.path.exists(cache_path):
            osm_feat = utils.get_existing_osm_feat(i[0], cache_path)
        else:
            osm_feat = utils.get_osm_feat.submit(i, checkpoint_dir=output_dir/"osm_geojsons"/"cache")
        time.sleep(0.1)
        task_results.append(osm_feat)

    results_df = utils.process.submit(task_results, task_list, task_results_path)
    return results_df.result()

overwrite = not use_existing_raw_osm
results_df = osm_features_flow(unique_task_list, overwrite=overwrite)



# ==========================================================

# rebuild original task list to populate results for duplicate tasks
results_df = results_df.merge(active_task_df[['unique_id', 'clean_link']], on='unique_id', how='left')
results_df.drop(columns=['unique_id'], inplace=True)
results_df = active_task_df[['unique_id', 'clean_link']].merge(results_df, on='clean_link', how='left')
results_df.drop(columns=['clean_link'], inplace=True)

# join processing results with existing data and validate results
output_df = feature_prep_df.merge(results_df, on="unique_id", how="left")
output_df['feature'] = output_df['feature_x'].where(output_df['feature_x'].notnull(), output_df['feature_y'])
output_df['flag'] = output_df['flag_x'].where(output_df['flag_x'].notnull(), output_df['flag_y'])
output_df.drop(columns=['feature_x', 'feature_y', 'flag_x', 'flag_y'], inplace=True)


# prepare data for next steps and csv outputs

error_ids = output_df.loc[output_df.feature.isnull() & output_df.flag.notnull(), "id"]
error_df = output_df.loc[output_df.id.isin(error_ids)].copy()
error_df.loc[error_df.flag.isnull(), "flag"] = "valid feature in project with at least one other invalid feature"

valid_df = output_df.loc[~output_df.id.isin(error_ids)].copy()

unprocessed_df = output_df[output_df.feature.isnull() & output_df.flag.isnull()].copy()

print("\t{} errors found out of {} tasks ({} were not procesed)".format(len(error_ids), len(output_df), len(unprocessed_df)))
utils.save_df(valid_df, processing_valid_path)
utils.save_df(error_df, processing_errors_path)


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
