"""
python3

Rebuild OSM features from OSM urls

OSM features are identified as either relations, ways, or nodes (OSM directions can also be parsed)

"""

import sys
import os
import math
import shutil
import json
import configparser
import itertools
from pathlib import Path

from shapely.geometry import MultiPolygon
from shapely.ops import unary_union
import pandas as pd
import geopandas as gpd

import utils


# ensure correct working directory when running as a batch parallel job
# in all other cases user should already be in project directory
if not hasattr(sys, 'ps1'):
    os.chdir(os.path.dirname(__file__))

# read config file
config = configparser.ConfigParser()
config.read('config.ini')

base_dir = Path(config["main"]["base_directory"])
run_name = config["main"]["active_run_name"]
github_name, github_repo, github_branch = config["main"]["github_name"], config["main"]["github_repo"], config["main"]["github_branch"]

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


timestamp = utils.get_current_timestamp('%Y_%m_%d_%H_%M')


# directory where all outputs will be saved
output_dir = base_dir / "output_data" / release_name / "results" / timestamp
(output_dir / "geojsons").mkdir(parents=True, exist_ok=True)

api = utils.init_overpass_api()


# =====
import importlib
importlib.reload(utils)
# =====

if __name__ == "__main__":

    input_data_df = utils.load_input_data(base_dir, release_name, output_project_fields, id_field, location_field)

    link_df_path = output_dir / "osm_links.csv"
    feature_prep_df_path = output_dir / "feature_prep.csv"

    if from_existing:
        full_feature_prep_df = utils.init_existing(output_dir, from_existing_timestamp)

    else:
        link_df = utils.get_osm_links(input_data_df, osm_str, invalid_str_list, update_ids)

        # save dataframe with all osm links to csv
        # invalid osm links can be referenced later for fixes
        link_df.to_csv(link_df_path, index=False, encoding="utf-8")

        # drop all rows with invalid osm links
        valid_link_df = link_df.loc[link_df.valid].copy(deep=True)

        print(f"""
        {len(input_data_df)} projects provides
        {len(set(link_df.id))} contain OSM links
        {len(link_df.loc[~link_df.valid])} non-parseable links over {len(set(link_df.loc[~link_df.valid, 'id']))} projects
        {len(valid_link_df)} valid links over {len(set(valid_link_df.id))} projects
        {len(set.intersection(set(link_df.loc[~link_df.valid, 'id']), set(valid_link_df.id)))} projects contain both valid and invalid links
        """)

        full_feature_prep_df = utils.classify_osm_links(valid_link_df)



    # option to sample data for testing; sample size <=0 returns full dataset
    feature_prep_df = utils.sample_features(full_feature_prep_df, sample_size=2)

    print(feature_prep_df.osm_type.value_counts())

    # get svg path for osm "directions" links
    svg_results = utils.generate_svg_paths(feature_prep_df, overwrite=False)

    # join svg paths back to dataframe
    for unique_id, svg_path in svg_results:
        feature_prep_df.loc[unique_id, "svg_path"] = svg_path



    feature_prep_df.to_csv(feature_prep_df_path, index=False)

    if prepare_only:
        sys.exit("Completed preparing feature_prep_df.csv, and exiting as `prepare_only` option was set.")



    # -------------------------------------
    # -------------------------------------


    def generate_task_list(df, api):
        # generate list of tasks to iterate over
        task_list = list(zip(
            df["unique_id"],
            df["clean_link"],
            df["osm_type"],
            df["osm_id"],
            df["svg_path"],
            # itertools.repeat(driver),
            itertools.repeat(api)
        ))
        return task_list

    task_list = generate_task_list(feature_prep_df, api)


    print("Running feature generation")
    # get_osm_feat for each row in feature_prep_df
    #     - parallelize
    #     - buffer lines/points to create polygons
    #     - convert all features to multipolygons

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


    processing_valid_path = output_dir / "processing_valid.csv"
    processing_errors_path = output_dir / "processing_errors.csv"

    valid_df.drop('feature', axis=1).to_csv(processing_valid_path, index=False)
    valid_df.to_csv(output_dir / "processing_valid_gdf.csv", index=False)
    errors_df.to_csv(processing_errors_path, index=False)


    # -------------------------------------
    # -------------------------------------


    print("Building GeoJSONs")

    # combine features for each project
    #    - iterate over all polygons (p) within feature multipolygons (mp) to create single multipolygon per project

    grouped_df = valid_df.groupby("project_id")["feature"].apply(list).reset_index(name="feature_list")
    # for group in grouped_df:
    #     group_mp = MultiPolygon([p for mp in group.feature for p in mp]).__geo_interface_
    # move this to apply instead of loop so we can have a final df to output results/errors to
    grouped_df["multipolygon"] = grouped_df.feature_list.apply(lambda mp_list: unary_union([p for mp in mp_list for p in mp.geoms]))
    grouped_df["multipolygon"] = grouped_df.multipolygon.apply(lambda x: MultiPolygon([x]) if x.type == "Polygon" else x)
    grouped_df["feature_count"] = grouped_df.feature_list.apply(lambda mp: len(mp))
    grouped_df["geojson_path"] = grouped_df.project_id.apply(lambda x: output_dir / "geojsons" / f"{x}.geojson")


    # join original project fields back to be included in geojson properties
    grouped_df = grouped_df.merge(input_data_df, left_on="project_id", right_on=id_field, how="left")


    # -----
    # create individual geojsons
    for ix, row in grouped_df.iterrows():
        path, geom, props = utils.prepare_single_feature(row)
        utils.output_single_feature_geojson(geom, props, path)

    if update_mode:
        # copy geojsons from update_timestamp geojsons dir to current timestamp geojsons dir
        update_target_geojsons = base_dir / "output_data" / release_name / "results" / update_timestamp / "geojsons"
        for gj in update_target_geojsons.iterdir():
            if int(gj.name.split(".")[0]) not in grouped_df.project_id.values:
                shutil.copy(gj, output_dir / "geojsons")

    # -----
    # create combined GeoJSON for all data
    combined_gdf = pd.concat([gpd.read_file(gj) for gj in (output_dir / "geojsons").iterdir()])

    # add github geojson urls
    combined_gdf["viz_geojson_url"] = combined_gdf.id.apply(lambda x: f"https://github.com/{github_name}/{github_repo}/blob/{github_branch}/latest/geojsons/{x}.geojson")
    combined_gdf["dl_geojson_url"] = combined_gdf.id.apply(lambda x: f"https://raw.githubusercontent.com/{github_name}/{github_repo}/{github_branch}/latest/geojsons/{x}.geojson")

    # date fields can get loaded a datetime objects which can geopandas doesn't always like to output, so convert to string to be safe
    for c in combined_gdf.columns:
        if c.endswith("Date (MM/DD/YYYY)"):
            combined_gdf[c] = combined_gdf[c].apply(lambda x: str(x))

    combined_gdf.to_file(output_dir / "all_combined_global.geojson", driver="GeoJSON")

    # -----
    # create combined GeoJSON  for each finance type
    for i in set(combined_gdf.finance_type):
        print(i)
        subgrouped_df = combined_gdf[combined_gdf.finance_type == i].copy()
        subgrouped_df.to_file(output_dir / f"{i}_combined_global.geojson", driver="GeoJSON")


    # -----
    # create final csv
    drop_cols = ['geometry']
    combined_gdf.drop(drop_cols, axis=1).to_csv(os.path.join(output_dir, "final_df.csv"), index=False)



    # -------------------------------------
    # -------------------------------------


    # -----
    # final summary output
    print(f"""
    Dataset complete: {timestamp}
    \t{output_dir}
    To set this as latest dataset: \n\t bash {base_dir}/set_latest.sh {release_name} {timestamp}
    """)
