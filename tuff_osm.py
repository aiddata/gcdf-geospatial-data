"""
python3

Rebuild OSM features from OSM urls

OSM features are identified as either relations, ways, or nodes (OSM directions can also be parsed)

"""

import sys
import os
import math
import json
import configparser
import itertools
from pathlib import Path

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

parallel = config.getboolean(run_name, "parallel")
max_workers = int(config[run_name]["max_workers"])

sample_size = int(config[run_name]["sample_size"])

# fields from input csv
id_field = config[run_name]["id_field"]
location_field = config[run_name]["location_field"]

# search string used to identify relevant OSM link within the location_field of input csv
osm_str = config[run_name]["osm_str"]
invalid_str_list = json.loads(config[run_name]["invalid_str_list"])

output_project_fields = json.loads(config[run_name]["output_project_fields"])


prepare_only = config.getboolean(run_name, "prepare_only")

from_existing = config.getboolean(run_name, "from_existing")
if from_existing:
    from_existing_timestamp = config[run_name]["from_existing_timestamp"]

update_mode = config.getboolean(run_name, "update_mode")
update_ids = None
if update_mode:
    update_ids = json.loads(config[run_name]["update_ids"])
    update_timestamp = config[run_name]["update_timestamp"]


# ==========================================================


timestamp = utils.get_current_timestamp('%Y_%m_%d_%H_%M')

base_dir = Path(base_dir)

# directory where all outputs will be saved
output_dir = base_dir / "output_data" / release_name / "results" / timestamp

utils.init_output_dir(output_dir)

feature_prep_df_path = output_dir / "feature_prep.csv"
processing_valid_path = output_dir / "processing_valid.csv"
processing_errors_path = output_dir / "processing_errors.csv"


api = utils.init_overpass_api()

input_data_df = utils.load_input_data(base_dir, release_name, output_project_fields, id_field, location_field)

if from_existing:
    full_feature_prep_df = utils.init_existing(output_dir, from_existing_timestamp, update_mode, update_ids)
    if update_mode:
        full_feature_prep_df = utils.subset_by_id(full_feature_prep_df, update_ids)

else:
    link_df = utils.get_osm_links(input_data_df, osm_str, invalid_str_list, output_dir=output_dir)

    full_feature_prep_df = utils.classify_osm_links(link_df)

    utils.osm_type_summary(link_df, full_feature_prep_df, summary=True)


# option to sample data for testing; sample size <=0 returns full dataset
sampled_feature_prep_df = utils.sample_features(full_feature_prep_df, sample_size=2)

feature_prep_df = utils.generate_svg_paths(sampled_feature_prep_df, overwrite=False)

utils.save_df(feature_prep_df, feature_prep_df_path)

if prepare_only:
    sys.exit("Completed preparing feature_prep_df.csv, and exiting as `prepare_only` option was set.")


# ==========================================================


def generate_task_list(df, api):
    # generate list of tasks to iterate over
    task_list = list(zip(
        df["unique_id"],
        df["clean_link"],
        df["osm_type"],
        df["osm_id"],
        df["svg_path"],
        itertools.repeat(api),
        df["version"]
    ))
    return task_list



print("Running feature generation")
# get_osm_feat for each row in feature_prep_df
#     - parallelize
#     - buffer lines/points to create polygons
#     - convert all features to multipolygons

task_list = generate_task_list(feature_prep_df, api)

valid_df = None
errors_df = None
iteration = 0
while errors_df is None or len(errors_df) > 0:

    iteration += 1
    # handle potential memory / task conflict issues by reducing workers as attempts increase
    iter_max_workers = math.ceil(max_workers / iteration)

    if errors_df is not None:
        task_list = generate_task_list(errors_df)

    # task_results = []
    # for result in utils.run_tasks(get_osm_feat, task_list, parallel, max_workers=max_workers, chunksize=1, unordered=True):
    #     task_results.append(result)

    task_results = utils.run_tasks(utils.get_osm_feat, task_list, parallel, max_workers=iter_max_workers, chunksize=1)

    valid_df, errors_df = utils.process_results(task_results, valid_df, errors_df, feature_prep_df)

    if iter_max_workers == 1 or iteration >= 5 or len(set(errors_df.message)) == 1 and "IndexError" in list(set(errors_df.message))[0]:
        break


utils.save_df(valid_df, processing_valid_path)
utils.save_df(errors_df, processing_errors_path)

importlib.reload(utils)

# ==========================================================


print("Building GeoJSONs")

grouped_df = utils.prepare_multipolygons(valid_df)

grouped_df["geojson_path"] = grouped_df.id.apply(lambda x: output_dir / "geojsons" / f"{x}.geojson")

# join original project fields back to be included in geojson properties
grouped_df = grouped_df.merge(input_data_df, on='id', how="left")


# create individual geojsons
for ix, row in grouped_df.iterrows():
    path, geom, props = utils.prepare_single_feature(row)
    utils.output_single_feature_geojson(geom, props, path)


# create combined GeoJSON for all data
combined_gdf = utils.load_all_geojsons(output_dir)

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
