
import os
import time
import datetime
import json
import requests
import random
import warnings
import functools
import re

from bs4 import BeautifulSoup as BS
from shapely.geometry import Point, Polygon, LineString, MultiPolygon
from shapely.ops import unary_union
import pandas as pd
import overpass
from overpass.errors import TimeoutError, ServerLoadError, MultipleRequestsError
import osm2geojson
from selenium import webdriver


def init_overpass_api():
    # initialize overpass api on all processes
    headers = {
        'Accept-Charset': 'utf-8;q=0.7,*;q=0.7',
        'From': 'geo@aiddata.wm.edu',
        'Referer': 'https://aiddata.org/',
        'User-Agent': 'overpass-api-python-wrapper (Linux x86_64)'
    }
    api = overpass.API(timeout=600, headers=headers)
    return api


def load_input_data(base_dir, release_name):
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
def classify_osm_links(filtered_df, quiet=True):
    tmp_feature_df_list = []
    for ix, (_, row) in enumerate(filtered_df.iterrows()):
        project_id = row["id"]
        if not quiet:
            print(project_id)
        osm_links = row["osm_list"]
        # iterate over each osm link for a way
        for link in osm_links:
            osm_id = None
            svg_path = None
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
            if not quiet:
                print(f"\t{osm_type} {osm_id}")
            tmp_feature_df_list.append([project_id, clean_link, osm_type, osm_id, svg_path])


    feature_df = pd.DataFrame(tmp_feature_df_list, columns=["project_id", "clean_link", "osm_type", "osm_id", "svg_path"])
    feature_df["unique_id"] = range(len(feature_df))
    feature_df["index"] = range(len(feature_df))

    feature_df.set_index('index', inplace=True)

    return feature_df


def sample_features(df, sample_size):
    """sample features from each osm link type
    """
    if sample_size <= 0:
        sample_df = df.copy(deep=True)
    else:
        sample_df = df.groupby('osm_type').apply(lambda x: x.sample(n=sample_size)).reset_index(drop=True)
    sample_df['index'] = sample_df['unique_id']
    sample_df.set_index('index', inplace=True)
    return sample_df


def generate_svg_paths(feature_prep_df, overwrite=False):

    if overwrite:
        overwrite_query = 1
    else:
        overwrite_query = feature_prep_df.svg_path.isnull()

    task_list = feature_prep_df.loc[(feature_prep_df.osm_type == "directions") & overwrite_query][['unique_id', 'clean_link']].values

    results = []

    if len(task_list) > 0:
        driver = create_web_driver()

        for unique, clean_link in task_list:
            print(clean_link)
            d = None
            attempts = 0
            max_attempts = 5
            while not d and attempts < max_attempts:
                attempts += 1
                try:
                    d = get_svg_path(clean_link, driver)
                except Exception as e:
                    print(f"\tAttempt {attempts}/{max_attempts}", repr(e))
            results.append([unique, d])
            # feature_prep_df.loc[ix, "svg_path"] = d

        driver.quit()

    return results


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


def create_web_driver():
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
    return driver


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


def get_node_geom(clean_link):
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


def get_way_geom(clean_link):
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
        node_feat = get_node_geom(node_url)
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


def get_relation_geom(osm_id, osm_type, api):
    # add OSM url check to make sure relation is still current
    # if not, does overpass allow us to access deleted/historical versions?
    #
    result = get_from_overpass(osm_id, osm_type, api)
    geo_list = osm2geojson.json2shapes(result)
    feat = unary_union([geom["shape"].buffer(0.00001) for geom in geo_list])
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


def get_directions_geom(url, d):
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
def get_osm_feat(unique_id, clean_link, osm_type, osm_id, svg_path, api):
    print(unique_id, osm_type)
    if osm_type == "directions":
        feat = get_directions_geom(clean_link, svg_path)
    elif osm_type == "node":
        feat = get_node_geom(clean_link)
    elif osm_type == "way":
        feat = get_way_geom(clean_link)
    elif osm_type == "relation":
        feat = get_relation_geom(osm_id, osm_type, api)
    else:
        raise Exception(f"Invalid OSM type in link ({osm_type})", unique_id, None)
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


def generate_feature_properties(row):
    props = {
        "id": row.project_id,
        "feature_count": row.feature_count,
    }
    for k,v in row.items():
        if k not in ["project_id", "feature_list", "feature_count", "multipolygon", "geojson_path", "geometry"]:
            if isinstance(v, type(pd.NaT)) or pd.isnull(v):
                v = None
            elif type(v) not in [int, str, float]:
                v = str(v)
            props[k] = v
    return props


def prepare_single_feature(row):
    """Export each MultiPolygon to individual GeoJSON
        - tuff id as filename
        - properties: project_id, count of locations, anything else?
    """
    geom = row.multipolygon.__geo_interface__
    props = generate_feature_properties(row)
    path = row.geojson_path
    return (path, geom, props)


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
