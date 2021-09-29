"""
python3

Rebuild OSM features from OSM urls

OSM features are identified as either relations, ways, or nodes (OSM directions can also be parsed)

"""

import sys
import os
import re
import time
import math
import shutil
import datetime
import json
import requests
import random
import warnings
import configparser
import functools
from pathlib import Path

from bs4 import BeautifulSoup as BS
from shapely.geometry import Point, Polygon, LineString, MultiPolygon
from shapely.ops import cascaded_union
import pandas as pd
import geopandas as gpd

import overpass
from overpass.errors import TimeoutError, ServerLoadError, MultipleRequestsError
import osm2geojson

from selenium import webdriver



# artifact from when working dir was manually specified
base_dir = Path(".")

# initialize overpass api on all processes
headers = {
    'Accept-Charset': 'utf-8;q=0.7,*;q=0.7',
    'From': 'geo@aiddata.wm.edu',
    'Referer': 'https://aiddata.org/',
    'User-Agent': 'overpass-api-python-wrapper (Linux x86_64)'
}
api = overpass.API(timeout=600, headers=headers)



# ensure correct working directory when running as a batch parallel job
# in all other cases user should already be in project directory
if not hasattr(sys, 'ps1'):
    os.chdir(os.path.dirname(__file__))

# read config file
config = configparser.ConfigParser()
config.read('config.ini')

parallel = config.getboolean('main', "parallel")
max_workers = int(config["main"]["max_workers"])
release_name = config["main"]["release_name"]

# fields from input csv
id_field = config["main"]["id_field"]
location_field = config["main"]["location_field"]

# search string used to identify relevant OSM link within the location_field of input csv
osm_str = config["main"]["osm_str"]


def load_input_data():
    """Loads input datasets from various Excel sheets

    Makes column names uniform
    Creates field to indicate source dataset

    Returns:
        pandas.DataFrame: combined input dataframe
    """
    # read in separate datasets
    development_df = pd.read_excel(base_dir / "input_data" / release_name / "AidDatasGlobalChineseDevelopmentFinanceDatasetv2.0_forseth.xlsx", sheet_name=0)
    military_df = pd.read_excel(base_dir / "input_data" / release_name / "AidDatasGlobalChineseMilitaryFinanceDataset.xlsx", sheet_name=0)
    huawei_df = pd.read_excel(base_dir / "input_data" / release_name / "AidDatasGlobalHuaweiFinanceDataset.xlsx", sheet_name=0)
    # rename non-matching columns
    military_df.rename(columns={'Recommended For Military Aggregates': 'Recommended For Aggregates'}, inplace=True)
    huawei_df.rename(columns={'Recommended For Huawei Aggregates': 'Recommended For Aggregates'}, inplace=True)
    # add field to indicate source dataset
    development_df["finance_type"] = "development"
    military_df["finance_type"] = "military"
    huawei_df["finance_type"] = "huawei"
    # merge datasets
    input_data = pd.concat([development_df, military_df, huawei_df], axis=0)
    return input_data


def get_current_timestamp(format_str=None):
    """Get the current timestamp

    Args:
        format_str (str, optional): string to format timestamp

    Returns:
        str: string formatted timestamp
    """
    if format_str is None:
        format_str = '%Y_%m_%d_%H_%M'
    timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime(format_str)
    return timestamp


def split_and_match_text(text, split, match):
    """Split a string and return matching elements

    Args:
        text (str): string to split and match
        split (str): string to split text on
        match (str): string to match split elements against

    Returns:
        list: list of matching elements
    """
    link_list = [i for i in text.split(split) if match in i]
    return link_list


def get_soup(url, pretty_print=False, timeout=60):
    """Extract a parseable representation of a web page

    Will sleep and attempt to retry for a set amount of time if request fails to complete

    Args:
        url (str): url of web page to extract parseable representation from
        pretty_print (bool, optional): flag to enable pretty print of response
        timeout (int, optional): number of seconds to wait for response before timing out

    Returns:
        bs4.BeautifulSoup: parseable representation of web page

    Raises:
        requests.exceptions.HTTPError: if response status code is not 200
        requests.exceptions.Timeout: if request times out
    """
    timer = 0
    page = None
    while not page or page.status_code != 200:
        try:
            page = requests.get(url)
            if page.status_code != 200:
                raise requests.exceptions.HTTPError(f"Request failed - Status code: {page.status_code} - URL: {url}")
        except:
            if timer >= timeout:
                raise requests.exceptions.Timeout(f"Timeout exceeded waiting for request ({url})")
            else:
                time.sleep(2)
                timer += 2
    soup = BS(page.text, 'html.parser')
    if pretty_print:
        # clean view of page contents if needed for finding html objects/classes
        print(soup.prettify())
    return soup


def get_node(clean_link):
    """Manage getting OSM node coordinates from OSM feature url

    Uses historical version of node URL if current URL no longer exists

    Args:
        clean_link (str): url of OSM feature

    Returns:
        shapely.geometry.Point: point geometry of node
    """
    try:
        soup = get_soup(clean_link)
        feat = build_node_geom(soup)
    except Exception as e:
        print(f"\tusing old node version: {clean_link} \n\t\t {repr(e)}")
        soup = get_soup(clean_link+"/history")
        feat = build_node_geom(soup)
    return feat


def build_node_geom(soup):
    """Recontruct node geometry

    Args:
        soup (bs4.BeautifulSoup): parseable representation of OSM feature page

    Returns:
        shapely.geometry.Point: point geometry of node
    """
    lon = soup.find("span", {"class":"longitude"}).text
    lat = soup.find("span", {"class":"latitude"}).text
    coords = [float(lon), float(lat)]
    # generate geojson compatible geometry string for feature
    feat = Point(coords)
    return feat


def build_way_geom(soup):
    """Reconstruct a way geometry

    Args:
        soup (bs4.BeautifulSoup): parseable representation of OSM feature page

    Returns:
        shapely.geometry.LineString, shapely.geometry Polygon: line or polygon geometry of way
    """
    # get list of nodes from page
    nodes_html = soup.find_all('a', {'class': 'node'})
    node_ids = [i["href"].split("/")[-1] for i in nodes_html]
    # iterate over nodes listed on way's html
    coords = []
    for node in node_ids:
        # build url for specific node
        node_url = f"https://www.openstreetmap.org/node/{node}"
        node_feat = get_node(node_url)
        node_coords = node_feat.coords[0]
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

    Args:
        osm_id (str): OSM id of feature to query
        osm_type (str): OSM type of feature to query
        api (overpass.API): Overpass API instance to use for queries

    Returns:
        overpas API query result
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
            time.sleep(random.randint(30, 120))
    return result


def get_svg_path(url, driver, max_attempts=10):
    """Get SVG path data from leaflet map at specified url

    Only specifically tested using OpenStreetMap 'directions' results

    Sleeps for a set amount of time if request fails to complete, until max attempts are reached

    Args:
        url (str): url of OSM directions page with leaflet map to extract path data from
        driver (webdriver): Selenium webdriver instance to use for web requests
        max_attempts (int, optional): number of times to attempt request before timing out

    Returns:
        str: SVG path element data
    """
    driver.get(url)
    attempts = 0
    d = None
    while not d:
        time.sleep(3)
        soup = BS(driver.page_source, "html.parser")
        try:
            d = soup.find("path", {"class": "leaflet-interactive"})["d"]
        except:
            if attempts >= max_attempts:
                raise Exception("max_attempts exceeded waiting for page to load")
            else:
                attempts += 1
    return d


def calculate_unit_size(start, end, first, last):
    """Calculate the size of an arbitrary geospatial unit in terms of decimal degrees

    start and end are decimal degree coordinates and
    first and last are corresponding points in arbitrary unit

    Examine difference between longitute and latitute for each pairing to
    determine value of 1 unit in decimal degrees for both longitude and latitude

    Args:
        start (tuple): start point coordinate pair (lon, lat)
        end (tuple): end point coordinate pair (lon, lat)
        first (tuple): first point pixel pair (x, y)
        last (tuple): last point pixel pair (x, y)

    Returns:
        float, float: size of each pixel unit in decimal degrees for longitude and latitude
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

    Args:
        url (str): url of OSM directions page
        d (str): SVG path element data

    Returns:
        shapely.geometry.LineString: line geometry of directions
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


def write_json_to_file(json_dict, path, **kwargs):
    """Write a valid JSON formatted dictionary to a file

    Args:
        json_dict (dict): dictionary to write to file
        path (str): path to write file to
        **kwargs: additional keyword arguments to pass to json.dump
    """
    file = open(path, "w")
    json.dump(json_dict, file, **kwargs)
    file.close()


def output_single_feature_geojson(geom, props, path):
    """Output a geojson file containing a single feature

    Args:
        geom (shapely.geometry.base.BaseGeometry): geometry of feature
        props (dict): dictionary of properties for feature
        path (str): path to write file to
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

    Args:
        geom_list (list): list of geometry of features
        props_list (list): list of dictionaries of properties for features
        path (str): path to write file to
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
    """Buffer a shapely geometry to create a Polygon if not already a Polygon or MultiPolygon

    Args:
        shapely.geometry.base.BaseGeometry: shapely geometry

    Returns:
        shapely.geometry.base.BaseGeometry: shapely Polygon or MultiPolygon
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        feat = fn(*args, **kwargs)
        if feat.type != "MultiPolygon":
            feat = MultiPolygon([feat])
        return feat
    return wrapper


def buffer_osm_feat(fn):
    """Buffer a shapely geometry to create a Polygon if not already a Polygon or MultiPolygon

    Args:
        shapely.geometry.base.BaseGeometry: shapely geometry

    Returns:
        shapely.geometry.base.BaseGeometry: shapely Polygon or MultiPolygon
    """
    @functools.wraps(fn)
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
        feat = get_node(clean_link)
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
        geo_list = osm2geojson.json2shapes(result)
        feat = cascaded_union([geom["shape"].buffer(0.00001) for geom in geo_list])
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


def run_tasks(func, flist, parallel, max_workers=None, chunksize=1):
    # run all downloads (parallel and serial options)
    wrapper_list = [(func, i) for i in flist]
    if parallel:
        # see: https://mpi4py.readthedocs.io/en/stable/mpi4py.futures.html
        # and: https://docs.python.org/3/library/concurrent.futures.html
        try:
            from mpi4py.futures import MPIPoolExecutor
            mpi = True
        except:
            from concurrent.futures import ProcessPoolExecutor
            mpi = False
        if max_workers is None:
            if mpi:
                if "OMPI_UNIVERSE_SIZE" not in os.environ:
                    raise ValueError("Parallel set to True and mpi4py is installed but max_workers not specified and OMPI_UNIVERSE_SIZE env var not found")
                max_workers = os.environ["OMPI_UNIVERSE_SIZE"]
                warnings.warn(f"Parallel set to True (mpi4py is installed) but max_workers not specified. Defaulting to OMPI_UNIVERSE_SIZE env var value ({max_workers})")
            else:
                import multiprocessing
                max_workers = multiprocessing.cpu_count()
                warnings.warn(f"Parallel set to True (mpi4py is not installed) but max_workers not specified. Defaulting to CPU count ({max_workers})")
        if mpi:
            with MPIPoolExecutor(max_workers=max_workers) as executor:
                results_gen = executor.starmap(_task_wrapper, wrapper_list, chunksize=chunksize)
        else:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                results_gen = executor.map(_task_wrapper, *zip(*wrapper_list), chunksize=chunksize)
        results = list(results_gen)
    else:
        results = []
        # for i in flist:
            # results.append(func(*i))
        for i in wrapper_list:
            results.append(_task_wrapper(*i))
    return results


if __name__ == "__main__":

    timestamp = get_current_timestamp('%Y_%m_%d_%H_%M')

    input_data = load_input_data()

    # directory where all outputs will be saved
    output_dir = base_dir / "output_data" / release_name
    results_dir = output_dir / "results" / timestamp
    os.makedirs(os.path.join(results_dir, "geojsons"), exist_ok=True)


    # prepare_only = True
    # from_existing = False
    # from_existing_timestamp = "2021_07_31_16_40"

    prepare_only = config.getboolean('main', "prepare_only")
    from_existing = config.getboolean('main', "from_existing")
    from_existing_timestamp = config["main"]["from_existing_timestamp"]

    update_mode = config.getboolean('main', "update_mode")
    if update_mode:
        update_ids = json.loads(config['main']["update_ids"])
        update_timestamp = config['main']["update_timestamp"]

    if from_existing:
        from_existing_path = base_dir / "output_data" / release_name / "results" / from_existing_timestamp / "feature_df.csv"
        feature_df = pd.read_csv(from_existing_path)

        # copy previously generated files to directory for current run
        shutil.copyfile(base_dir / "output_data" / release_name / "results" / from_existing_timestamp / "osm_links_df.csv", results_dir / "osm_links_df.csv")
        shutil.copyfile(base_dir / "output_data" / release_name / "results" / from_existing_timestamp / "osm_error_df.csv", results_dir / "osm_error_df.csv")

    else:

        raw_df = input_data.copy()

        loc_df = raw_df[[id_field, location_field]].copy(deep=True)
        loc_df.columns = ["id", "location"]

        if update_mode:
            loc_df = loc_df[loc_df["id"].isin(update_ids)]

        loc_df_not_null = loc_df.loc[~loc_df.location.isnull()]
        osm_df = loc_df_not_null.loc[loc_df_not_null.location.str.contains(osm_str)].copy()


        osm_df["osm_list"] = osm_df.location.apply(lambda x: split_and_match_text(x, " ", osm_str))


        # save dataframe with osm links to csv
        osm_df.to_csv(os.path.join(results_dir, "osm_links_df.csv"), index=False, encoding="utf-8")

        invalid_str_list = ["search", "query"]
        errors_df = osm_df[osm_df.osm_list.apply(lambda x: any(i in str(x) for i in invalid_str_list))].copy(deep=True)
        errors_df.to_csv(os.path.join(results_dir, "osm_error_df.csv"), index=False, encoding="utf-8")

        filtered_df = osm_df.loc[~osm_df.index.isin(errors_df.index)].copy(deep=True)


        print("{} out of {} projects contain OSM link(s)".format(len(osm_df), len(loc_df)))

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

        tmp_feature_df_list = []
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
                    tmp_feature_df_list.append([tuff_id, clean_link, osm_type, osm_id, svg_path])
                except Exception as e:
                    print(f"\tError: {link}")
                    print("\t", e)


        feature_df = pd.DataFrame(tmp_feature_df_list, columns=["tuff_id", "clean_link", "osm_type", "osm_id", "svg_path"])
        feature_df["unique_id"] = range(len(feature_df))



    if "directions" in set(feature_df.loc[feature_df.svg_path.isnull(), "osm_type"]):

        # chromedriver_path = "./chromedriver"
        # options = webdriver.ChromeOptions()
        # options.binary_location = "./chrome-linux/chrome"
        # options.headless = True
        # driver = webdriver.Chrome(executable_path=chromedriver_path, options=options)

        geckodriver_path = "./geckodriver"
        options = webdriver.FirefoxOptions()
        options.headless = True
        profile = webdriver.FirefoxProfile()
        profile.accept_untrusted_certs = True
        options.binary_location = "./firefox/firefox-bin"

        # import random
        # if parallel:
        #     sleep_val = random.randint(0, max_workers)
        #     print(f"Sleeping for {sleep_val} seconds before starting...")
        #     time.sleep(sleep_val)

        driver = webdriver.Firefox(executable_path=geckodriver_path, options=options, firefox_profile=profile)

        driver.set_window_size(1920*10, 1080*10)


        for ix, row in feature_df.loc[feature_df.osm_type == "directions"].iterrows():
            print(row.clean_link)
            d = None
            attempts = 0
            max_attempts = 5
            while not d and attempts < max_attempts:
                attempts += 1
                try:
                    d = get_svg_path(row.clean_link, driver)
                except Exception as e:
                    print(f"\tAttempt {attempts}/{max_attempts}", repr(e))
            feature_df.loc[ix, "svg_path"] = d

        driver.quit()


    feature_df_path = os.path.join(results_dir, "feature_df.csv")
    feature_df.to_csv(feature_df_path, index=False)


    if prepare_only:
        sys.exit("Completed preparing feature_df.csv, and exiting as `directions_only` option was set.")


    # ---------
    # column name for join field in original df
    results_join_field_name = "unique_id"
    # position of join field in each tuple in task list
    results_join_field_loc = 0
    # ---------

    def gen_flist(df):
        # generate list of tasks to iterate over
        flist = list(zip(
            df["unique_id"],
            df["clean_link"],
            df["osm_type"],
            df["osm_id"],
            df["svg_path"],
            # itertools.repeat(driver),
            # itertools.repeat(api)
        ))
        return flist

    flist = gen_flist(feature_df)

    valid_df = None
    errors_df = None
    iteration = 0
    while errors_df is None or len(errors_df) > 0:

        iteration += 1

        if errors_df is not None:
            flist = gen_flist(errors_df)

        print("Running feature generation")
        # get_osm_feat for each row in feature_df
        #     - parallelize
        #     - buffer lines/points
        #     - convert all features to multipolygons

        # results = []
        # for result in run_tasks(get_osm_feat, flist, parallel, max_workers=max_workers, chunksize=1, unordered=True):
        #     results.append(result)
        iter_max_workers = math.ceil(max_workers / iteration)

        results = run_tasks(get_osm_feat, flist, parallel, max_workers=iter_max_workers, chunksize=1)


        print("Completed feature generation")

        # join function results back to df
        results_df = pd.DataFrame(results, columns=["status", "message", results_join_field_name, "feature"])
        # results_df.drop(["feature"], axis=1, inplace=True)
        results_df[results_join_field_name] = results_df[results_join_field_name].apply(lambda x: x[results_join_field_loc])

        output_df = feature_df.merge(results_df, on=results_join_field_name, how="left")


        if valid_df is None:
            valid_df = output_df[output_df["status"] == 0].copy()
        else:
            valid_df = pd.concat([valid_df, output_df.loc[output_df.status == 0]])


        skipped_df = output_df[~output_df["status"].isin([0, 1])].copy()

        errors_df = output_df[output_df["status"] > 0].copy()
        print("\t{} errors found out of {} tasks".format(len(errors_df), len(output_df)))

        if iter_max_workers == 1 or iteration >= 5 or len(set(errors_df.message)) == 1 and "IndexError" in list(set(errors_df.message))[0]:
            break

    #
    # errors_df = valid_df.loc[valid_df.osm_type.isin(["rel", "relation"])].copy()
    # valid_df = valid_df.loc[valid_df.osm_type != "relation"].copy()
    # flist = gen_flist(errors_df)
    # results = run_tasks(get_osm_feat, flist, parallel, max_workers=iter_max_workers, chunksize=1)
    #


    errors_df.to_csv(os.path.join(results_dir, "processing_errors_df.csv"), index=False)

    # output valid results to csv
    valid_df[[i for i in valid_df.columns if i != "feature"]].to_csv(results_dir / "valid_df.csv", index=False)
    valid_df.to_csv(results_dir / "valid_gdf.csv", index=False)


    # -------------------------------------
    # -------------------------------------

    print("Building GeoJSONs")

    # combine features for each project
    #    - iterate over all polygons (p) within feature multipolygons (mp) to create single multipolygon per project

    grouped_df = valid_df.groupby("tuff_id")["feature"].apply(list).reset_index(name="feature_list")
    # for group in grouped_df:
    #     group_mp = MultiPolygon([p for mp in group.feature for p in mp]).__geo_interface_
    # move this to apply instead of loop so we can have a final df to output results/errors to
    grouped_df["multipolygon"] = grouped_df.feature_list.apply(lambda mp_list: cascaded_union([p for mp in mp_list for p in mp]))
    grouped_df["multipolygon"] = grouped_df.multipolygon.apply(lambda x: MultiPolygon([x]) if x.type == "Polygon" else x)
    grouped_df["feature_count"] = grouped_df.feature_list.apply(lambda mp: len(mp))
    grouped_df["geojson_path"] = grouped_df.tuff_id.apply(lambda x: os.path.join(results_dir, "geojsons", f"{x}.geojson"))


    # join original project fields back to be included in geojson properties
    project_data_df = input_data.copy()
    project_fields = ["AidData TUFF Project ID", "Recommended For Aggregates", "Umbrella", "Title", "Status", "Implementation Start Year", "Completion Year", "Flow Type", "Flow Class", "AidData Sector Name", "Commitment Year", "Funding Agencies", "Receiving Agencies", "Implementing Agencies", "Recipient", "Amount (Constant USD2017)", "Planned Implementation Start Date (MM/DD/YYYY)", "Planned Completion Date (MM/DD/YYYY)", "Actual Implementation Start Date (MM/DD/YYYY)", "Actual Completion Date (MM/DD/YYYY)", "finance_type"]
    project_data_df = project_data_df[project_fields]
    grouped_df = grouped_df.merge(project_data_df, left_on="tuff_id", right_on="AidData TUFF Project ID", how="left")

    # valid_df_with_proj = valid_df.merge(project_data_df, left_on="tuff_id", right_on="AidData TUFF Project ID", how="left")


    def build_feature(row):
        """Export each MultiPolygon to individual GeoJSON
            - tuff id as filename
            - properties: tuff_id, count of locations, anything else?
        """
        geom = row.multipolygon.__geo_interface__
        props = prepare_properties(row)
        path = row.geojson_path
        output_single_feature_geojson(geom, props, path)


    def prepare_properties(row):
        props = {
            "id": row.tuff_id,
            "feature_count": row.feature_count,
        }
        for k,v in row.items():
            if k not in ["tuff_id", "feature_list", "feature_count", "multipolygon", "geojson_path", "geometry"]:
                if isinstance(v, type(pd.NaT)) or pd.isnull(v):
                    v = None
                elif type(v) not in [int, str, float]:
                    v = str(v)
                props[k] = v
        return props


    for ix, row in grouped_df.iterrows():
        build_feature(row)


    if update_mode:
        # copy geojsons from update_timestamp geojsons dir to current timestamp geojsons dir
        update_target_geojsons = base_dir / "output_data" / release_name / "results" / update_timestamp / "geojsons"
        for gj in update_target_geojsons.iterdir():
            if int(gj.name.split(".")[0]) not in grouped_df.tuff_id.values:
                shutil.copy(gj, results_dir / "geojsons")


    # create combined GeoJSON for all data and for each finance type
    combined_gdf = pd.concat([gpd.read_file(gj) for gj in (results_dir / "geojsons").iterdir()])

    combined_gdf.to_file(results_dir / "all_combined_global.geojson", driver="GeoJSON")
    for i in set(combined_gdf.finance_type):
        print(i)
        subgrouped_df = combined_gdf[combined_gdf.finance_type == i].copy()
        subgrouped_df.to_file(results_dir / f"{i}_combined_global.geojson", driver="GeoJSON")



    # add github geojson url to df and save to csv
    combined_gdf["viz_geojson_url"] = combined_gdf.id.apply(lambda x: f"https://github.com/aiddata/china-osm-geodata/blob/master/latest/geojsons/{x}.geojson")
    combined_gdf["dl_geojson_url"] = combined_gdf.id.apply(lambda x: f"https://raw.githubusercontent.com/aiddata/china-osm-geodata/master/latest/geojsons/{x}.geojson")

    drop_cols = ['tuff_id', 'feature_list', 'multipolygon', 'feature_count', 'geojson_path', 'geometry']
    combined_gdf[[i for i in combined_gdf.columns if i not in drop_cols]].to_csv(os.path.join(results_dir, "final_df.csv"), index=False)

    print(f"Dataset complete: {timestamp}")
    print(f"\t{results_dir}")
    print(f"To set this as latest dataset: \n\t bash {base_dir}/set_latest.sh {release_name} {timestamp}")



