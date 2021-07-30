"""
python3

Rebuild OSM features from OSM urls

OSM features are identified as either relations, ways, or nodes (OSM directions can also be parsed)

"""

import os
import re
import time
import datetime
import json
import requests
import warnings
import itertools
from functools import wraps

from bs4 import BeautifulSoup as BS
from requests.api import get
from shapely.geometry import Point, Polygon, LineString, MultiPolygon, shape
import pandas as pd

import overpass
from overpass.errors import TimeoutError, ServerLoadError, MultipleRequestsError
import osm2geojson

from selenium import webdriver

# -----------------
# user variables

mode = "parallel"
# mode = "serial"

max_workers = 59

# base_dir = "/home/userw/Desktop/tuff_osm"
base_dir = "/sciclone/home20/smgoodman/tuff_osm"


id_field = "AidData Tuff Project ID"
location_field = "Geographic Location"


# release_name = "test"
# input_csv_name = "tuff_osm_test.csv"

# release_name = "2.0prerelease"
# input_csv_name = "ChineseOfficialFinance2.0_PreliminaryDataset_July222021.csv"

release_name = "prerelease_20210730"
input_csv_name = "exportforseth.csv"

# -----------------

osm_str = "https://www.openstreetmap.org/"

input_csv_path = os.path.join(base_dir, "input_data", release_name, input_csv_name)

output_dir = os.path.join(base_dir, "output_data", release_name)



# =====================================


def get_current_timestamp(format_str=None):
    if format_str is None:
        format_str = '%Y_%m_%d_%H_%M'
    timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)
    return timestamp


def extract_links_from_text(text, match):
    link_list = [i for i in text.split(" ") if match in i]
    return link_list


def get_soup(url, pretty_print=False):
    """Extract a parseable representation of a web page
    """
    timeout = 60
    timer = 0
    page = None
    while not page or page.status_code != 200:
        try:
            page = requests.get(url)
            if page.status_code != 200:
                raise Exception(f"Request failed - Status code: {page.status_code} - URL: {url}")
        except:
            if timer >= timeout:
                raise Exception(f"Timeout exceeded waiting for request ({url})")
            else:
                time.sleep(2)
                timer += 2
    soup = BS(page.text, 'html.parser')
    if pretty_print:
        # clean view of page contents if needed for finding html objects/classes
        print(soup.prettify())
    return soup


def get_node(clean_link):
    """Manage getting node feature and coordinates from url
    """
    try:
        soup = get_soup(clean_link)
        feat, coords = build_node_geom(soup)
    except Exception as e:
        print(f"\tusing old node version: {clean_link} \n\t\t {repr(e)}")
        soup = get_soup(clean_link+"/history")
        feat, coords = build_node_geom(soup)
    return feat, coords


def build_node_geom(soup):
    """Recontruct node geometry
    """
    lon = soup.find("span", {"class":"longitude"}).text
    lat = soup.find("span", {"class":"latitude"}).text
    coords = [float(lon), float(lat)]
    # generate geojson compatible geometry string for feature
    feat = Point(coords)
    return feat, coords


def build_way_geom(soup):
    """Reconstruct a way geometry
    """
    # get list of nodes from page
    nodes_html = soup.find_all('a', {'class': 'node'})
    node_ids = [i["href"].split("/")[-1] for i in nodes_html]
    # iterate over nodes listed on way's html
    coords = []
    for node in node_ids:
        # build url for specific node
        node_url = f"https://www.openstreetmap.org/node/{node}"
        _, node_coords = get_node(node_url)
        coords.append(node_coords)
    # generate geojson compatible geometry string for feature
    #   - currently assumes they are either linestrings or polygons
    #   - polygons are identifed by having the same starting and end node
    if node_ids[0] == node_ids[-1]:
        feat = Polygon(coords)
    else:
        feat = LineString(coords)
    return feat


def get_from_overpass(osm_id, osm_type, api):
    """Query Overpass API and sleep if there are timeout related errors
    https://github.com/mvexel/overpass-api-python-wrapper/blob/cda548262d94f1f14dfd9c8ca21e369dd9254248/overpass/errors.py
    """
    if osm_type == "relation":
        osm_type = "rel"
    if osm_type not in ["rel", "way", "node"]:
        raise ValueError(f"Invalid type to query from OSM Overpass API ({osm_type})")
    ix = 0
    while 1:
        try:
            result = api.get(f"{osm_type}({osm_id});(._;>>;);", responseformat="json")
            break
        except (TimeoutError, ServerLoadError, MultipleRequestsError):
            if ix > 10: raise
            ix += 1
            time.sleep(60)
    return result



def calculate_unit_size(start, end, first, last):
    """Calculate the size of an arbitrary geospatial unit in terms of decimal degrees

    start and end are decimal degree coordinates and
    first and last are corresponding points in arbitrary unit

    Examine difference between longitute and latitute for each pairing to
    determine value of 1 unit in decimal degrees for both longitude and latitude
    """
    # absolute difference between starting and ending latitudes and longitudes in decimal degrees
    degree_diff_lat = abs(end[0] - start[0])
    degree_diff_lon = abs(end[1] - start[1])
    # absolute difference between first and last arbitrary units
    pixel_diff_lat = abs(last[1] - first[1])
    pixel_diff_lon = abs(last[0] - first[0])
    # calculate value of each unit in decimal degrees for both latitude and longitude
    lat_unit_val = degree_diff_lat / pixel_diff_lat
    lon_unit_val = degree_diff_lon / pixel_diff_lon
    return lon_unit_val, lat_unit_val


def build_directions_geom(url, d):
    """Build a shapely LineString from the SVG path data in the url
    """
    # get the svg path data
    # d = None
    # attempts = 0
    # while not d and attempts < 5:
    #     d = get_svg_path(url)
    #     attempts += 1
    # SVG path from directions starts with "M" and has "L" between "lon lat" pairs
    # e.g., M321 116L333 122L373 113
    pixel_coords_strs = d[1:].split("L")
    # convert SVG string to int tuples (x, y)
    pixel_coords_tuples = [tuple(map(int, i.split(" "))) for i in pixel_coords_strs]
    # x, y for each
    first, last = pixel_coords_tuples[0], pixel_coords_tuples[-1]
    # get coordinates from url
    #   coords in format: '<start_lat>%2C<start_lon>%3B<end_lat>%2C<end_lon>'
    #   example: '0.1673%2C35.1329%3B0.1054%2C35.0865'
    raw_coords_str_from_url = url.split("&route=")[1]
    start_str, end_str = raw_coords_str_from_url.split("%3B")
    # lat, lon for each
    start = tuple(map(float, start_str.split("%2C")))
    end = tuple(map(float, end_str.split("%2C")))
    # calculate unit size for SVG path data
    lon_unit_val, lat_unit_val = calculate_unit_size(start, end, first, last)
    # conver to decimal degrees
    # currently in epsg 3857 (web mercator)
    final_coords = []
    for ix, i in enumerate(pixel_coords_tuples):
        if ix == 0:
            final_coords.append((start[1], start[0]))
        # elif ix == len(pixel_coords_tuples) - 1:
            # final_coords.append((end))
        else:
            prev_coords = final_coords[ix-1]
            j = pixel_coords_tuples[ix-1]
            final_coords.append( (
                prev_coords[0] + 1 * (i[0] - j[0]) * lon_unit_val,
                prev_coords[1] + -1 * (i[1] - j[1]) * lat_unit_val
            ))
    # convert coords to shapely feature
    feat = LineString(final_coords)
    return feat


def write_json_to_file(json_dict, path):
    """Write a valid JSON formatted dictionary to a file
    """
    file = open(path, "w")
    json.dump(json_dict, file, indent=4)
    file.close()


def output_single_feature_geojson(geom, props, path):
    """Output a geojson file containing a single feature
    """
    # build geojson format dictionary
    geojson_dict = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": props,
                "geometry": geom,
            }
        ]
    }
    write_json_to_file(geojson_dict, path)


def output_multi_feature_geojson(geom_list, props_list, path):
    """Ouput a geojson file containing a multiple features
    """
    features = []
    for geom, props in zip(geom_list, props_list):
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": geom,
        })
    geojson_dict = {
        "type": "FeatureCollection",
        "features": features,
    }
    write_json_to_file(geojson_dict, path)


def convert_osm_feat_to_multipolygon(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        feat = fn(*args, **kwargs)
        if feat.type != "MultiPolygon":
            feat = MultiPolygon([feat])
        return feat
    return wrapper


def buffer_osm_feat(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        feat = fn(*args, **kwargs)
        if feat.type not in ["Polygon", "MultiPolygon"]:
            feat = feat.buffer(0.00001)
        return feat
    return wrapper

@convert_osm_feat_to_multipolygon
@buffer_osm_feat
def get_osm_feat(unique_id, clean_link, osm_type, osm_id, svg_path):
    print(unique_id, osm_type)
    if osm_type == "directions":
        # feat = build_directions_geom(clean_link, driver)
        feat = build_directions_geom(clean_link, svg_path)
    elif osm_type == "node":
        feat, _ = get_node(clean_link)
    elif osm_type == "way":
        try:
            soup = get_soup(clean_link)
            feat = build_way_geom(soup)
        except Exception as e:
            print(f"\tusing old way version: {clean_link} \n\t\t {repr(e)}")
            soup = get_soup(clean_link)
            details = soup.find_all('div', {'class': 'details'})
            if "Deleted" not in details[0].text:
                print(f"\tNo previous way version found ({osm_id})")
                raise
            history_link = clean_link + "/history"
            soup = get_soup(history_link)
            history_soup = soup.find_all('div', {'class': 'browse-way'})
            # currently just grabs first historical version, but
            # could need to iterate if that version was bad for some reason
            feat = build_way_geom(history_soup[1])
    elif osm_type == "relation":
        # add OSM url check to make sure relation is still current
        # if not, does overpass allow us to access deleted/historical versions?
        #
        result = get_from_overpass(osm_id, osm_type, api)
        geo = osm2geojson.json2shapes(result)
        # drop nodes that are sometimes parts of relations (e.g. capital is node, along with ADM bounds)
        good_geo = [i for i in geo if i["properties"]["type"] != "node"]
        # if relation is actually just nodes, it should be empty after initial pass
        # so now rebuild with the nodes included
        # (this is typically just for a multi-point geometry such as a wind farm)
        if not good_geo:
            good_geo = [i for i in geo if i["properties"]["type"]]
        feat = good_geo[0]["shape"]
    else:
        raise Exception(f"Invalid OSM type in link ({osm_type})", unique_id, None)
    return feat


def _task_wrapper(func, args):
    try:
        result = func(*args)
        return (0, "Success", args, result)
    except Exception as e:
        # raise
        return (1, repr(e), args, None)


def run_tasks(func, flist, mode, max_workers=None, chunksize=1):
    # run all downloads (parallel and serial options)
    wrapper_list = [(func, i) for i in flist]
    if mode == "parallel":
        # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
        from mpi4py.futures import MPIPoolExecutor
        if max_workers is None:
            if "OMPI_UNIVERSE_SIZE" not in os.environ:
                raise ValueError("Mode set to parallel but max_workers not specified and OMPI_UNIVERSE_SIZE env var not found")
            max_workers = os.environ["OMPI_UNIVERSE_SIZE"]
            warnings.warn(f"Mode set to parallel but max_workers not specified. Defaulting to OMPI_UNIVERSE_SIZE env var value ({max_workers})")
        with MPIPoolExecutor(max_workers=max_workers) as executor:
            # results_gen = executor.starmap(func, flist, chunksize=chunksize)
            results_gen = executor.starmap(_task_wrapper, wrapper_list, chunksize=chunksize)
        results = list(results_gen)
    else:
        results = []
        # for i in flist:
            # results.append(func(*i))
        for i in wrapper_list:
            results.append(_task_wrapper(*i))
    return results


def get_svg_path(url, driver=None):
    """Get SVG path data from leaflet map at specified url

    Only specifically tested using OpenStreetMap 'directions' results
    """
    print(url)
    driver.get(url)
    max_attempts = 10
    attempts = 0
    d = None
    while not d:
        time.sleep(2)
        print(attempts)
        soup = BS(driver.page_source, "html.parser")
        try:
            d = soup.find("path", {"class": "leaflet-interactive"})["d"]
        except:
            if attempts >= max_attempts:
                raise Exception("max_attempts exceeded waiting for page to load")
            else:
                attempts += 1
    return d


# =============================================================================


api = overpass.API(timeout=600)




if __name__ == "__main__":

    timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

    results_dir = os.path.join(output_dir, "results", timestamp)
    os.makedirs(os.path.join(results_dir, "geojsons"), exist_ok=True)

    raw_df = pd.read_csv(input_csv_path)
    loc_df = raw_df[[id_field, location_field]].copy(deep=True)
    loc_df.columns = ["id", "location"]

    loc_df_not_null = loc_df.loc[~loc_df.location.isnull()]
    osm_df = loc_df_not_null.loc[loc_df_not_null.location.str.contains(osm_str)].copy()


    osm_df["osm_list"] = osm_df.location.apply(lambda x: extract_links_from_text(x, osm_str))


    # save dataframe with osm links to csv
    osm_df.to_csv(os.path.join(results_dir, "osm_links_df.csv"), index=False, encoding="utf-8")

    invalid_str_list = ["search", "query"]
    errors_df = osm_df[osm_df.osm_list.apply(lambda x: any(i in str(x) for i in invalid_str_list))].copy(deep=True)
    errors_df.to_csv(os.path.join(results_dir, "osm_error_df.csv"), index=False, encoding="utf-8")

    filtered_df = osm_df.loc[~osm_df.index.isin(errors_df.index)].copy(deep=True)


    print("{} out of {} projects contain OSM link(s)".format(len(osm_df), len(raw_df)))

    print("{} out of {} projects with OSM link(s) had at least 1 non-parseable link ({} valid)".format(len(errors_df), len(osm_df), len(filtered_df)))

    """
    # directions link validation

    directions_df = osm_df[osm_df.osm_list.apply(lambda x: any(i in str(x) for i in ["directions"]))].copy(deep=True)
    directions_links = [j for i in directions_df.osm_list for j in i if "directions" in j]
    assert len(directions_links) == sum(["&route=" in i for i in directions_links])

    # list of coords in format: '<start_lon>%2C<start_lat>%3B<end_lon>%2C<end_lat>'
    # example: '0.1673%2C35.1329%3B0.1054%2C35.0865'
    raw_coords_list = [i.split("&route=")[1].split("#map=")[0] for i in directions_links]

    assert 0 == sum([len(i.split("%3B")) != 2 for i in raw_coords_list])
    """

    """
    # temporary for testing

    node_list = [j for i in osm_df["osm_list"].to_list() for j in i if "node" in str(j)]
    way_list = [j for i in osm_df["osm_list"].to_list() for j in i if "way" in str(j)]
    relation_list = [j for i in osm_df["osm_list"].to_list() for j in i if "relation" in str(j)]
    directions_list = [j for i in osm_df["osm_list"].to_list() for j in i if "directions" in str(j)]
    search_list = [j for i in osm_df["osm_list"].to_list() for j in i if "search" in str(j)]
    query_list = [j for i in osm_df["osm_list"].to_list() for j in i if "query" in str(j)]
    """



    feature_df_list = []
    for ix, (_, row) in enumerate(filtered_df.iterrows()):
        # if ix < 170: continue
        tuff_id = row["id"]
        print(tuff_id)
        osm_links = row["osm_list"]
        # iterate over each osm link for a way
        for link in osm_links:
            osm_id = None
            svg_path = None
            try:
                # extract if link is for a way, node, relation, directions
                osm_type = link.split("/")[3].split("?")[0]
                if osm_type == "directions":
                    osm_id = None
                    clean_link = link.split("#map=")[0]
                    clean_link = clean_link[clean_link.index("http"):]
                    while not clean_link[-1].isdigit():
                        clean_link = clean_link[:-1]
                elif osm_type in ["node", "way", "relation"]:
                    # extract the osm id for the way/node/relation from the url
                    #   (gets rid of an extra stuff after the id as well)
                    # osm_id = link.split("/")[4].split("#")[0].split(".")[0]
                    osm_id = re.match("([0-9]*)", link.split("/")[4]).groups()[0]
                    # rebuild a clean link
                    clean_link = f"https://www.openstreetmap.org/{osm_type}/{osm_id}"
                print(f"\t{osm_type} {osm_id}")
                feature_df_list.append([tuff_id, clean_link, osm_type, osm_id, svg_path])
            except Exception as e:
                print(f"\tError: {link}")
                print("\t", e)


    feature_df = pd.DataFrame(feature_df_list, columns=["tuff_id", "clean_link", "osm_type", "osm_id", "svg_path"])
    feature_df["unique_id"] = range(len(feature_df))

    # #
    # error_list = [53541, 52518, 40359, 39370, 32153, 61166, 33488, 53522, 39001, 33402, 46364]
    # feature_df = feature_df.loc[feature_df.tuff_id.isin(error_list)].copy(deep=True)
    # #

    feature_df = feature_df.loc[feature_df.osm_type != "directions"].copy(deep=True)

    if "directions" in set(feature_df.osm_type):

        # chromedriver_path = "/sciclone/home20/smgoodman/tuff_osm/chromedriver"
        # options = webdriver.ChromeOptions()
        # options.binary_location = "/sciclone/home20/smgoodman/tuff_osm/chrome-linux/chrome"
        # options.headless = True
        # driver = webdriver.Chrome(executable_path=chromedriver_path, options=options)

        # geckodriver_path = "/home/userw/Desktop/tuff_osm/geckodriver"
        geckodriver_path = "/sciclone/home20/smgoodman/tuff_osm/geckodriver"
        options = webdriver.FirefoxOptions()
        options.headless = True
        profile = webdriver.FirefoxProfile()
        profile.accept_untrusted_certs = True
        # options.binary_location = "/home/userw/Desktop/tuff_osm/firefox/firefox"
        options.binary_location = "/sciclone/home20/smgoodman/tuff_osm/firefox/firefox-bin"

        # import random
        # if mode == "parallel":
        #     sleep_val = random.randint(0, max_workers)
        #     print(f"Sleeping for {sleep_val} seconds before starting...")
        #     time.sleep(sleep_val)

        driver = webdriver.Firefox(executable_path=geckodriver_path, options=options, firefox_profile=profile)

        driver.set_window_size(1920*10, 1080*10)


        feature_df.loc[feature_df.osm_type == "directions", "svg_path"] = feature_df.loc[feature_df.osm_type == "directions"].clean_link.apply(lambda x: get_svg_path(x, driver))

        driver.quit()


    feature_df_path = os.path.join(results_dir, "feature_df.csv")
    feature_df.to_csv(feature_df_path, index=False)

    # |||||||||||||||||||||
    # TESTING SUBSET

    # 73914  directions relation
    # 73153  node relation way
    # feature_df = feature_df.loc[feature_df.tuff_id.isin([73914, 73153])]

    # feature_df = feature_df[3600:3650].copy(deep=True)
    # |||||||||||||||||||||

    # feature_df = feature_df.loc[feature_df.osm_type != "directions"].copy(deep=True)


        # get_osm_feat for each row in feature_df
        #     - parallelize
        #     - buffer lines/points
        #     - convert all features to multipolygons

    # generate list of tasks to iterate over
    flist = list(zip(
        feature_df["unique_id"],
        feature_df["clean_link"],
        feature_df["osm_type"],
        feature_df["osm_id"],
        feature_df["svg_path"],
        # itertools.repeat(driver),
        # itertools.repeat(api)
    ))



    print("Running feature generation")

    results = run_tasks(get_osm_feat, flist, mode, max_workers=max_workers, chunksize=1)


    print("Completed feature generation")

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

    output_df = feature_df.merge(results_df, on=results_join_field_name, how="left")

    print("Results:")

    errors_df = output_df[output_df["status"] != 0]
    print("\t{} errors found out of {} tasks".format(len(errors_df), len(output_df)))




    # for ix, row in errors_df.iterrows():
    #     print("\t", row)


    # output results to csv
    output_simple_path = os.path.join(results_dir, "results_df.csv")
    output_df[[i for i in output_df.columns if i != "feature"]].to_csv(output_simple_path, index=False)

    output_path = os.path.join(results_dir, "results_features_df.csv")
    output_df.to_csv(output_path, index=False)


    # -------------------------------------
    # -------------------------------------

    invalid_tuff_id_list = list(set(output_df.loc[output_df.status == 1].tuff_id))

    valid_df = output_df.loc[~output_df.tuff_id.isin(invalid_tuff_id_list)].copy(deep=True)

    print("Building GeoJSONs")

    # combine features for each project
    #    - iterate over all polygons (p) within feature multipolygons (mp) to create single multipolygon per project

    grouped_df = valid_df.groupby("tuff_id")["feature"].apply(list).reset_index(name="feature_list")
    # for group in grouped_df:
    #     group_mp = MultiPolygon([p for mp in group.feature for p in mp]).__geo_interface_
    # move this to apply instead of loop so we can have a final df to output results/errors to
    grouped_df["multipolygon"] = grouped_df.feature_list.apply(lambda mp_list: MultiPolygon([p for mp in mp_list for p in mp]))
    grouped_df["feature_count"] = grouped_df.multipolygon.apply(lambda mp: len(mp))
    grouped_df["geojson_path"] = grouped_df.tuff_id.apply(lambda x: os.path.join(results_dir, "geojsons", f"{x}.geojson"))


    def build_feature(row):
        """Export each MultiPolygon to individual GeoJSON
            - tuff id as filename
            - properties: tuff_id, count of locations, anything else?
        """
        geom = row.multipolygon.__geo_interface__
        props = {
            "id": row.tuff_id,
            "feature_count": row.feature_count,
        }
        path = row.geojson_path
        output_single_feature_geojson(geom, props, path)


    for ix, row in grouped_df.iterrows():
        build_feature(row)



    # combine all MultiPolygons into one GeoJSON

    geom_list = grouped_df["multipolygon"].apply(lambda mp: mp.__geo_interface__)
    props_list = grouped_df.apply(lambda x: {"id": x.tuff_id, "feature_count": x.feature_count}, axis=1)
    path = os.path.join(results_dir, "combined.geojson")

    output_multi_feature_geojson(geom_list, props_list, path)


    print(f"Dataset complete: {timestamp}")
    print(f"\t{results_dir}")
