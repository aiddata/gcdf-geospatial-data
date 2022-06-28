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


output_project_fields = json.loads(config[run_name]["output_project_fields"])

prepare_only = config.getboolean(run_name, "prepare_only")
from_existing = config.getboolean(run_name, "from_existing")
from_existing_timestamp = config[run_name]["from_existing_timestamp"]

update_mode = config.getboolean(run_name, "update_mode")
if update_mode:
    update_ids = json.loads(config[run_name]["update_ids"])
    update_timestamp = config[run_name]["update_timestamp"]


timestamp = utils.get_current_timestamp('%Y_%m_%d_%H_%M')

# directory where all outputs will be saved
output_dir = base_dir / "output_data" / release_name
results_dir = output_dir / "results" / timestamp
os.makedirs(os.path.join(results_dir, "geojsons"), exist_ok=True)

api = utils.init_overpass_api()


# =====
import importlib
importlib.reload(utils)
# =====

if __name__ == "__main__":

    input_data = utils.load_input_data(base_dir, release_name)

    link_df_path = results_dir / "osm_links.csv"
    invalid_link_df_path = results_dir / "osm_invalid_links.csv"
    feature_prep_df_path = results_dir / "feature_prep.csv"

    if from_existing:
        existing_dir = base_dir / "output_data" / release_name / "results" / from_existing_timestamp
        existing_link_df_path = existing_dir / "osm_valid_links.csv"
        existing_invalid_link_df_path = existing_dir / "osm_invalid_links.csv"
        existing_feature_prep_df_path = existing_dir / "feature_prep.csv"

        full_feature_prep_df = pd.read_csv(existing_feature_prep_df_path)

        # copy previously generated files to directory for current run
        shutil.copyfile(existing_link_df_path, link_df_path)
        shutil.copyfile(existing_invalid_link_df_path, invalid_link_df_path)

    else:

        loc_df = input_data[[id_field, location_field]].copy(deep=True)
        loc_df.columns = ["id", "location"]

        if update_mode:
            loc_df = loc_df.loc[loc_df["id"].isin(update_ids)]

        # keep rows where location field contains at least one osm link
        link_df = loc_df.loc[loc_df.location.notnull() & loc_df.location.str.contains(osm_str)].copy(deep=True)
        # get osm links from location field
        link_df["osm_list"] = link_df.location.apply(lambda x: utils.split_and_match_text(x, " ", osm_str))
        # save dataframe with osm links to csv
        link_df.to_csv(os.path.join(results_dir, "osm_links.csv"), index=False, encoding="utf-8")

        # save all rows invalid osm links to separate csv that can be referenced for fixes
        invalid_str_list = ["search", "query"]
        invalid_link_df = link_df.loc[link_df.osm_list.apply(lambda x: any(i in str(x) for i in invalid_str_list))].copy(deep=True)
        invalid_link_df.to_csv(os.path.join(results_dir, "osm_invalid_links.csv"), index=False, encoding="utf-8")

        # drop all rows with invalid osm links
        valid_link_df = link_df.loc[~link_df.index.isin(invalid_link_df.index)].copy(deep=True)

        print(f"""
        {len(loc_df)} projects provides
        {len(link_df)} contain OSM links
        {len(invalid_link_df)} contain at least 1 non-parseable link
        {len(valid_link_df)} projects with valid links
        """)

        full_feature_prep_df = utils.classify_osm_links(valid_link_df)


    if sample_size <= 0:
        feature_prep_df = full_feature_prep_df.copy(deep=True)
    else:
        feature_prep_df = utils.sample_features(full_feature_prep_df, sample_size=sample_size)


    if "directions" in set(feature_prep_df.loc[feature_prep_df.svg_path.isnull(), "osm_type"]):

        driver = utils.create_web_driver()

        for ix, row in feature_prep_df.loc[feature_prep_df.osm_type == "directions"].iterrows():
            print(row.clean_link)
            d = None
            attempts = 0
            max_attempts = 5
            while not d and attempts < max_attempts:
                attempts += 1
                try:
                    d = utils.get_svg_path(row.clean_link, driver)
                except Exception as e:
                    print(f"\tAttempt {attempts}/{max_attempts}", repr(e))
            feature_prep_df.loc[ix, "svg_path"] = d

        driver.quit()


    feature_prep_df_path = os.path.join(results_dir, "feature_prep.csv")
    feature_prep_df.to_csv(feature_prep_df_path, index=False)



    # -------------------------------------
    # -------------------------------------


    if prepare_only:
        sys.exit("Completed preparing feature_prep_df.csv, and exiting as `prepare_only` option was set.")


    features_df = feature_prep_df.copy(deep=True)

    # -------------------------------------
    # -------------------------------------

    def gen_flist(df):
        # generate list of tasks to iterate over
        flist = list(zip(
            df["unique_id"],
            df["clean_link"],
            df["osm_type"],
            df["osm_id"],
            df["svg_path"],
            # itertools.repeat(driver),
            itertools.repeat(api)
        ))
        return flist

    flist = gen_flist(features_df)



    print("Running feature generation")

    valid_df = None
    errors_df = None
    iteration = 0
    while errors_df is None or len(errors_df) > 0:

        iteration += 1

        if errors_df is not None:
            flist = gen_flist(errors_df)

        # get_osm_feat for each row in features_df
        #     - parallelize
        #     - buffer lines/points
        #     - convert all features to multipolygons

        # results = []
        # for result in run_tasks(get_osm_feat, flist, parallel, max_workers=max_workers, chunksize=1, unordered=True):
        #     results.append(result)
        iter_max_workers = math.ceil(max_workers / iteration)

        results = utils.run_tasks(utils.get_osm_feat, flist, parallel, max_workers=iter_max_workers, chunksize=1)

        # ---------
        # column name for join field in original df
        results_join_field_name = "unique_id"
        # position of join field in each tuple in task list
        results_join_field_loc = 0
        # ---------

        # join function results back to df
        results_df = pd.DataFrame(results, columns=["status", "message", results_join_field_name, "feature"])
        # results_df.drop(["feature"], axis=1, inplace=True)
        results_df[results_join_field_name] = results_df[results_join_field_name].apply(lambda x: x[results_join_field_loc])

        output_df = features_df.merge(results_df, on=results_join_field_name, how="left")


        if valid_df is None:
            valid_df = output_df[output_df["status"] == 0].copy()
        else:
            valid_df = pd.concat([valid_df, output_df.loc[output_df.status == 0]])


        skipped_df = output_df[~output_df["status"].isin([0, 1])].copy()

        errors_df = output_df[output_df["status"] > 0].copy()
        print("\t{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

        if iter_max_workers == 1 or iteration >= 5 or len(set(errors_df.message)) == 1 and "IndexError" in list(set(errors_df.message))[0]:
            break


    errors_df.to_csv(os.path.join(results_dir, "processing_errors_df.csv"), index=False)

    # output valid results to csv
    valid_df[[i for i in valid_df.columns if i != "feature"]].to_csv(results_dir / "valid_df.csv", index=False)
    valid_df.to_csv(results_dir / "valid_gdf.csv", index=False)


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
    grouped_df["geojson_path"] = grouped_df.project_id.apply(lambda x: os.path.join(results_dir, "geojsons", f"{x}.geojson"))


    # join original project fields back to be included in geojson properties
    project_data_df = input_data[output_project_fields].copy()
    grouped_df = grouped_df.merge(project_data_df, left_on="project_id", right_on=id_field, how="left")


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
                shutil.copy(gj, results_dir / "geojsons")

    # -----
    # create combined GeoJSON for all data
    combined_gdf = pd.concat([gpd.read_file(gj) for gj in (results_dir / "geojsons").iterdir()])

    # add github geojson urls
    combined_gdf["viz_geojson_url"] = combined_gdf.id.apply(lambda x: f"https://github.com/{github_name}/{github_repo}/blob/{github_branch}/latest/geojsons/{x}.geojson")
    combined_gdf["dl_geojson_url"] = combined_gdf.id.apply(lambda x: f"https://raw.githubusercontent.com/{github_name}/{github_repo}/{github_branch}/latest/geojsons/{x}.geojson")

    # date fields can get loaded a datetime objects which can geopandas doesn't always like to output, so convert to string to be safe
    for c in combined_gdf.columns:
        if c.endswith("Date (MM/DD/YYYY)"):
            combined_gdf[c] = combined_gdf[c].apply(lambda x: str(x))

    combined_gdf.to_file(results_dir / "all_combined_global.geojson", driver="GeoJSON")

    # -----
    # create combined GeoJSON  for each finance type
    for i in set(combined_gdf.finance_type):
        print(i)
        subgrouped_df = combined_gdf[combined_gdf.finance_type == i].copy()
        subgrouped_df.to_file(results_dir / f"{i}_combined_global.geojson", driver="GeoJSON")


    # -----
    # create final csv
    drop_cols = ['project_id', 'feature_list', 'multipolygon', 'feature_count', 'geojson_path', 'geometry']
    combined_gdf[[i for i in combined_gdf.columns if i not in drop_cols]].to_csv(os.path.join(results_dir, "final_df.csv"), index=False)


    # -----
    # final summary output
    print(f"""
    Dataset complete: {timestamp}
    \t{results_dir}
    To set this as latest dataset: \n\t bash {base_dir}/set_latest.sh {release_name} {timestamp}
    """)
