
import os
import time
import datetime
import json
import requests
import random
import warnings
import functools
import re
import multiprocessing as mp

from bs4 import BeautifulSoup as BS
from shapely.geometry import Point, Polygon, LineString, MultiPolygon
from shapely.ops import unary_union
import pandas as pd
import geopandas as gpd
import overpass
from overpass.errors import TimeoutError, ServerLoadError, MultipleRequestsError
import osm2geojson
from selenium import webdriver

import prefect
from prefect import task
# from prefect import Client, task
# from prefect.engine import state


# def run_flow(flow, executor, prefect_cloud_enabled, project_name):

#     # flow.run_config = LocalRun()
#     flow.executor = executor

#     if prefect_cloud_enabled:
#         flow_id = flow.register(project_name=project_name)
#         client = Client()
#         run_id = client.create_flow_run(flow_id=flow_id)
#         state = run_id
#     else:
#         state = flow.run()

#     return state


def init_output_dir(output_dir):
    (output_dir / "geojsons").mkdir(parents=True, exist_ok=True)


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


def load_input_data(base_dir, release_name, output_project_fields, id_field, location_field, version_field=None):
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
    all_df = pd.concat([development_df, military_df, huawei_df], axis=0)

    all_df.dropna(axis=0, how='all', inplace=True)
    all_df.dropna(axis=1, how='all', inplace=True)

    input_df = all_df[output_project_fields].copy(deep=True)
    input_df['id'] = all_df[id_field]
    input_df['location'] = all_df[location_field]
    if version_field:
        input_df['version'] = all_df[version_field]
    else:
        input_df['version'] = None

    input_df['precision'] = "precise"

    return input_df


def load_simple_input_data(base_dir, release_name, output_project_fields, id_field, location_field, precision_field=None, version_field=None):
    """Loads input datasets from various Excel sheets

    Makes column names uniform
    Creates field to indicate source dataset

    Returns:
        pandas.DataFrame: combined input dataframe
    """
    # read in separate datasets
    all_df = pd.read_csv(base_dir / "input_data" / release_name / "OSM2018sorted.csv")

    all_df.dropna(axis=0, how='all', inplace=True)
    all_df.dropna(axis=1, how='all', inplace=True)

    # add field to indicate source dataset
    all_df["finance_type"] = "all"

    input_df = all_df[output_project_fields].copy(deep=True)
    input_df['id'] = all_df[id_field]
    input_df['location'] = all_df[location_field]
    if version_field:
        input_df['version'] = all_df[version_field]
    else:
        input_df['version'] = None

    if precision_field:
        input_df['precision'] = all_df[precision_field]
    else:
        input_df['precision'] = "precise"

    return input_df


# def init_existing(output_dir, existing_timestamp, update_mode=False, update_ids=None):
#     existing_dir = output_dir.parent / existing_timestamp

#     existing_feature_prep_df_path = existing_dir / "feature_prep.csv"
#     full_feature_prep_df = pd.read_csv(existing_feature_prep_df_path)

#     if update_mode:
#         # copy geojsons from update_timestamp geojsons dir to current timestamp geojsons dir
#         update_target_geojsons = existing_dir / "geojsons"
#         for gj in update_target_geojsons.iterdir():
#             if int(gj.name.split(".")[0]) not in update_ids:
#                 shutil.copy(gj, output_dir / "geojsons")

#         full_feature_prep_df = full_feature_prep_df.loc[full_feature_prep_df["id"].isin(update_ids)].copy()

#     return full_feature_prep_df


def create_unique_osm_id(row):
    if pd.isnull(row.osm_type):
        uid = None
    elif row.osm_type == 'directions':
        uid = row.clean_link
    elif pd.isnull(row.osm_version):
        try:
            uid = str(int(row.osm_id))
        except:
            print (row)
            raise
    else:
        uid = f'{int(row.osm_id)}_{row.osm_version}'

    return uid


def load_existing(existing_dir, link_df, use_existing_feature):

    df = link_df.copy()
    existing_feature_prep_path = existing_dir / "feature_prep.csv"
    existing_processing_valid_path = existing_dir / "processing_valid.csv"

    if existing_feature_prep_path.exists():
        # join svg_path col to current run
        existing_feature_prep_df = pd.read_csv(existing_feature_prep_path)
        svg_df = existing_feature_prep_df[['clean_link', 'svg_path']].loc[existing_feature_prep_df.svg_path.notnull()].copy()
        # deduplicate svg_df
        svg_df.drop_duplicates('clean_link', inplace=True)
        df = df.merge(svg_df, on='clean_link', how='left')

        if use_existing_feature and existing_processing_valid_path.exists():
            # join raw feature data to current run

            df['merge_field'] = df.apply(lambda x: create_unique_osm_id(x), axis=1)
            existing_processing_valid_df = pd.read_csv(existing_processing_valid_path)
            feature_df = existing_processing_valid_df.loc[existing_processing_valid_df.feature.notnull()].copy()
            feature_df['merge_field'] = feature_df.apply(lambda x: create_unique_osm_id(x), axis=1)
            feature_df = feature_df[['merge_field', 'feature']].copy()
            # deduplicate feature_df
            feature_df.drop_duplicates('merge_field', inplace=True)
            df = df.merge(feature_df, on='merge_field', how='left')
            df.drop(columns=['merge_field'], inplace=True)

    # if update_mode:
    #     df = df.loc[df["id"].isin(update_ids)].copy()

    return df


def split_and_match_text(text, split, match):
    """Split a string and return matching elements

    Args:
        text (str): string to split and match
        split (str): string to split text on
        match (str): string to match split elements against

    Returns:
        list: list of matching elements
    """
    if isinstance(split, str):
        link_list = [i for i in str(text).split(split) if match in i]
    elif isinstance(split, list):
        rsplit = re.compile("|".join(split)).split
        link_list = [i for i in rsplit(str(text)) if match in i]

    return link_list


def clean_osm_link(link, version=None):

    osm_id = None

    # extract if link is for a way, node, relation, directions
    osm_type = link.split("/")[3].split("?")[0]

    if osm_type == "directions":
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

    else:
        clean_link = "Invalid osm_type"


    return clean_link, osm_type, osm_id


def get_osm_links(base_df, osm_str, invalid_str_list=None, output_dir=None):

    link_list_df = base_df.copy(deep=True)


    # keep rows where location field contains at least one osm link
    link_list_df['has_osm_str'] = link_list_df.location.notnull() & link_list_df.location.str.contains(osm_str)
    # get osm links from location field
    link_list_df["osm_list"] = link_list_df['location'].apply(lambda x: split_and_match_text(x, [" ", "\n"], osm_str))

    link_list_df['has_precision_vals'] = link_list_df.precision.notnull()
    link_list_df["precision_list"] = link_list_df['precision'].apply(lambda x: [i for i in str(x).split(', ') if i != ''])

    link_list_df['osm_count'] = link_list_df['osm_list'].apply(lambda x: len(x))
    link_list_df['precision_count'] = link_list_df['precision_list'].apply(lambda x: len(x))

    # placeholder for if we ever utilize osm version history
    link_list_df["osm_version"] = link_list_df["version"]


    link_list_df['has_matching_counts'] = link_list_df['osm_count'] == link_list_df['precision_count']

    link_list_df['osm_tuple'] = link_list_df.loc[link_list_df.has_matching_counts].apply(lambda x: list(zip(x.osm_list, x.precision_list)), axis=1)


    link_df = link_list_df.explode('osm_tuple').copy(deep=True)

    link_df["unique_id"] = range(len(link_df))
    link_df["index"] = link_df["unique_id"]
    link_df.set_index('index', inplace=True)


    link_df[['osm_link', 'osm_precision']] = link_df.apply(lambda x: x.osm_tuple if isinstance(x.osm_tuple, tuple) else (None, None), axis=1, result_type='expand')


    link_df['valid'] = True
    if invalid_str_list:
        link_df.loc[~link_df.has_osm_str, 'valid'] = False
        link_df.loc[link_df.osm_link.isnull(), 'valid'] = False
        link_df.loc[link_df.valid & link_df.osm_link.str.contains('|'.join(invalid_str_list)), 'valid'] = False

    link_df.loc[link_df.osm_link == '', 'valid'] = False
    link_df.loc[~link_df.has_precision_vals, 'valid'] = False
    link_df.loc[~link_df.has_matching_counts, 'valid'] = False

    link_df[["clean_link", "osm_type", "osm_id"]] = link_df.apply(lambda x: clean_osm_link(x.osm_link, x.osm_version) if x.valid else (None, None, None), axis=1, result_type='expand')


    if output_dir:
        link_df_path = output_dir / "osm_links.csv"
        # save dataframe with all osm links to csv
        # invalid osm links can be referenced later for fixes
        link_df.to_csv(link_df_path, index=False, encoding="utf-8")

    return link_df


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


def osm_type_summary(df):

    summary_str = f"""
    {len(set(df.id))} projects provided
    {len(set(df.loc[df.has_osm_str].id))} projects contain OSM links
    {len(df.loc[df.has_osm_str & ~df.valid])} non-parseable osm links over {len(set(df.loc[df.has_osm_str & ~df.valid].id))} projects
    {len(df.loc[df.valid])} valid links over {len(set(df.loc[df.valid].id))} projects
    {len(set.intersection(set(df.loc[df.has_osm_str & ~df.valid, 'id']), set(df.loc[df.valid].id)))} projects contain both valid and invalid links

    Distribution of valid OSM link types:
    """
    for i,j in df.loc[df.valid].osm_type.value_counts().to_dict().items():
        summary_str += f'\n\t\t{i}: {j}'

    print(summary_str)


def sample_and_validate(df, sample_size, summary=True):
    """sample features from each osm link type
    """

    if summary:
        osm_type_summary(df)

    valid_df = df.loc[df.valid].copy()

    if sample_size <= 0:
        sample_df = valid_df.copy(deep=True)
    else:
        sample_df = valid_df.groupby('osm_type').apply(lambda x: x.sample(n=sample_size)).reset_index(drop=True)
    sample_df['index'] = sample_df['unique_id']
    sample_df.set_index('index', inplace=True)

    if 'svg_path' not in sample_df.columns:
        sample_df['svg_path'] = None
    if 'feature' not in sample_df.columns:
        sample_df['feature'] = None
    if 'flag' not in sample_df.columns:
        sample_df['flag'] = None

    return sample_df


def create_web_driver():

    # chromedriver_path = "./chromedriver"
    # options = webdriver.ChromeOptions()
    # options.binary_location = "./chrome-linux/chrome"
    # options.headless = True
    # driver = webdriver.Chrome(executable_path=chromedriver_path, options=options)

    geckodriver_path = "./geckodriver"
    options = webdriver.FirefoxOptions()
    options.headless = False
    # options.headless = True

    options.add_argument("--disable-extensions")
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-application-cache')
    options.add_argument('--disable-gpu')
    options.add_argument("--disable-dev-shm-usage")

    options.binary_location = "./firefox/firefox-bin"

    profile = webdriver.FirefoxProfile()
    profile.accept_untrusted_certs = True

    profile.set_preference("browser.cache.disk.enable", False)
    profile.set_preference("browser.cache.memory.enable", False)
    profile.set_preference("browser.cache.offline.enable", False)
    profile.set_preference("network.http.use-cache", False)

    # import random
    # if parallel:
    #     sleep_val = random.randint(0, max_workers)
    #     print(f"Sleeping for {sleep_val} seconds before starting...")
    #     time.sleep(sleep_val)

    global driver
    driver = webdriver.Firefox(executable_path=geckodriver_path, options=options, firefox_profile=profile)

    driver.set_window_size(1920*10, 1080*10)
    # return driver


def run_task(unique_id, clean_link):
    return [unique_id, get_svg_path(clean_link)]


def quit_driver(n):
    driver.quit()


def generate_svg_paths(feature_prep_df, overwrite=False, upper_limit=False, nprocs=1):

    feature_prep_df = feature_prep_df.copy()

    if overwrite or 'svg_path' not in feature_prep_df.columns:
        overwrite_query = 1
    else:
        overwrite_query = feature_prep_df.svg_path.isnull()

    task_list = feature_prep_df.loc[(feature_prep_df.osm_type == "directions") & overwrite_query][['unique_id', 'clean_link']].values

    if upper_limit:
        task_list = task_list[:upper_limit]


    if len(task_list) > 0:

        if len(task_list) < nprocs:
            nprocs = len(task_list)

        if nprocs > 1:

            with mp.Pool(nprocs, initializer=create_web_driver) as pool:
                results_list = list(pool.starmap(run_task, task_list))
                for p in results_list:
                    feature_prep_df.loc[p[0], "svg_path"] = p[1]
                _ = pool.map(quit_driver, range(nprocs))

        else:

            create_web_driver()

            for unique_id, clean_link in task_list:
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
                # results.append([unique_id, d])
                feature_prep_df.loc[unique_id, "svg_path"] = d

            driver.quit()

    return feature_prep_df


def get_svg_path(url, max_attempts=10):
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
    time.sleep(2)
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
    # driver.close()
    return d



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
                # raise requests.exceptions.Timeout(f"Timeout exceeded waiting for request ({url})")
                return -1
            else:
                time.sleep(2)
                timer += 2
    soup = BS(page.text, 'html.parser')
    if pretty_print:
        # clean view of page contents if needed for finding html objects/classes
        print(soup.prettify())
    return soup


def check_osm_version_number(soup, version):
    current_version = soup.find('h4', text=re.compile('Version')).text.replace('\n', '').split('#')[-1]
    version_match = int(current_version) == int(version)
    return version_match


def get_node_geom(clean_link):
    """Manage getting OSM node coordinates from OSM feature url

    Uses historical version of node URL if current URL no longer exists

    Args:
        clean_link (str): url of OSM feature

    Returns:
        shapely.geometry.Point: point geometry of node
    """
    soup = get_soup(clean_link)
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
        unique_id, feat, flag = fn(*args, **kwargs)
        if feat and feat.type != "MultiPolygon":
            feat = MultiPolygon([feat])
        return unique_id, feat, flag
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
        unique_id, feat, flag = fn(*args, **kwargs)
        if feat and feat.type not in ["Polygon", "MultiPolygon"]:
            feat = feat.buffer(0.00001)
        return unique_id, feat, flag
    return wrapper


# def handle_failure(task, old_state, new_state):
#     if isinstance(new_state, state.Failed):
#         return state.Success(result='unknown failure')
#     else:
#         return new_state


# @task(log_stdout=True, state_handlers=[handle_failure], task_run_name=lambda **kwargs: f"{kwargs['task'][1]}")
@task()
@convert_osm_feat_to_multipolygon
@buffer_osm_feat
def get_osm_feat(task):
    unique_id, clean_link, osm_type, osm_id, svg_path, api, version = task

    print(unique_id, osm_type)
    # logger = prefect.context.get("logger")
    # logger.info(clean_link)

    if osm_type == "directions":
        feat = get_directions_geom(clean_link, svg_path)
    else:
        soup = get_soup(clean_link)
        if soup == -1:
            return (unique_id, None, 'invalid url')

        deleted = soup.find(text=re.compile('Deleted')) is not None

        if deleted:
            return (unique_id, None, 'deleted')

        if version is None:
            version_match = True
        else:
            version_match = check_osm_version_number(soup, version)
            if not version_match:
                return (unique_id, None, 'version_mismatch')

        if osm_type == "node":
            feat = build_node_geom(soup)
        elif osm_type == "way":
            feat = build_way_geom(soup)
        elif osm_type == "relation":
            feat = get_relation_geom(osm_id, osm_type, api)
        else:
            return (unique_id, None, 'invalid osm type ({osm_type})')

    return (unique_id, feat, None)


@task
def process(r, t, output_path):
    combined = zip(r, t)
    results = []
    for rr, tt in combined:
        if rr == 'unknown failure':
            results.append((tt[0], None, 'unknown failure'))
        else:
            results.append(rr)

    results_df = pd.DataFrame(results, columns=["unique_id", "feature", "flag"])
    results_df.to_csv(output_path, index=False)


def save_df(df, path):
    """Save a dataframe to a csv file
    """
    df.to_csv(path, index=True)


def prepare_multipolygons(valid_df):

    # combine features for each project
    #    - iterate over all polygons (p) within feature multipolygons (mp) to create single multipolygon per project

    grouped_df = valid_df.groupby("id")["feature"].apply(list).reset_index(name="feature_list")
    # for group in grouped_df:
    #     group_mp = MultiPolygon([p for mp in group.feature for p in mp]).__geo_interface_
    # move this to apply instead of loop so we can have a final df to output results/errors to
    grouped_df["multipolygon"] = grouped_df.feature_list.apply(lambda mp_list: unary_union([p for mp in mp_list for p in mp.geoms]))
    grouped_df["multipolygon"] = grouped_df.multipolygon.apply(lambda x: MultiPolygon([x]) if x.type == "Polygon" else x)
    grouped_df["feature_count"] = grouped_df.feature_list.apply(lambda mp: len(mp))
    return grouped_df


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
        "id": row.id,
        "feature_count": row.feature_count,
    }
    for k,v in row.items():
        if k not in ['id', 'location', 'project_id', 'feature_list', 'feature_count', 'multipolygon', 'geojson_path', 'geometry']:
            if isinstance(v, type(pd.NaT)) or pd.isnull(v):
                v = None
            elif type(v) not in [int, str, float]:
                v = str(v)
            props[k] = v
    return props


def prepare_single_feature(row):
    """Export each MultiPolygon to individual GeoJSON
        - id as filename
        - properties: id, feature_count, original fields defined in config
    """
    geom = row.multipolygon.__geo_interface__
    props = generate_feature_properties(row)
    path = row.geojson_path
    return (path, geom, props)


def load_project_geojsons(output_dir, id_list):

    gdf_list = []
    for i in id_list:
        gj_path = output_dir / "geojsons" / f'{i}.geojson'
        gdf = gpd.read_file(gj_path)
        gdf_list.append(gdf)

    combined_gdf = pd.concat(gdf_list)

    # date fields can get loaded a datetime objects which can geopandas doesn't always like to output, so convert to string to be safe
    for c in combined_gdf.columns:
        if c.endswith("Date (MM/DD/YYYY)"):
            combined_gdf[c] = combined_gdf[c].apply(lambda x: str(x))

    return combined_gdf


def load_all_geojsons(output_dir):
    combined_gdf = pd.concat([gpd.read_file(gj) for gj in (output_dir / "geojsons").iterdir()])

    # date fields can get loaded a datetime objects which can geopandas doesn't always like to output, so convert to string to be safe
    for c in combined_gdf.columns:
        if c.endswith("Date (MM/DD/YYYY)"):
            combined_gdf[c] = combined_gdf[c].apply(lambda x: str(x))

    return combined_gdf


def export_combined_data(combined_gdf, output_dir):
    # export all combined GeoJSON and a subset for each finance type
    combined_gdf.to_file(output_dir / "all_combined_global.geojson", driver="GeoJSON")
    for i in set(combined_gdf.finance_type):
        print(i)
        subgrouped_df = combined_gdf[combined_gdf.finance_type == i].copy()
        subgrouped_df.to_file(output_dir / f"{i}_combined_global.geojson", driver="GeoJSON")

    # create final csv
    final_drop_cols = ['geometry']
    final_df = combined_gdf.drop(final_drop_cols, axis=1)
    final_path = output_dir / "final_df.csv"
    final_df.to_csv(final_path, index=False)


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
