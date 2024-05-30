
## Replication

In this readme we provide an overview of the generalized steps necessary to run the complete workflow. This included environmental setup, defining configuration settings, and other operational considerations. Once you have read this readme, if you wish to replicate the GeoGCDF v3 dataset be sure to read the [gcdf_v3.md readme](examples/replication/gcdf_v3.md).


### Technical Overview

The code in this repository utilizes a combination of webscrapping, APIs, and geospatial processing to extract geospatial features from OpenStreeMap (OSM) URLs which detail either OSM features or driving directions. Python is used for all primary processing (Shell scripts are also used to initialize parallel jobs and manage repository files) and has been tested with packages and dependencies managed by Anaconda (see section on setting up environment below). All code can be run on a local environment using a single process or by leveraging parallel processing (implemented using both [concurrent futures](https://docs.python.org/3/library/concurrent.futures.html), as well as [mpi4py](https://github.com/mpi4py/mpi4py) on [William & Mary's HPC](https://www.wm.edu/offices/it/services/researchcomputing/atwm/index.php)), which will substantially reduce the amount of time needed to run.

Python webscrapping is dependent on [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) and [Selenium](https://selenium-python.readthedocs.io/), and access to the OSM Overpass API leverages the [Overpass API Python wrapper](https://github.com/mvexel/overpass-api-python-wrapper). Additional geospatial processing and data management utilizes [Shapely](https://shapely.readthedocs.io/en/stable/manual.html), [osm2geojson](https://github.com/aspectumapp/osm2geojson) (we use a fork of osm2geojson modified for specific issues), and [Pandas](https://pandas.pydata.org/).

Project data prepared using AidData's TUFF (Tracking Underreport Financial Flows) methodology from the GCDF v3 serves an input and contains text descriptions, incuding OSM URLs, of locations associated with Chinese financed projects which are identified by a unique "AidData Tuff Project ID". For each project, OSM URLs are extracted (projects may have zero or multiple OSM URLs) and converted into geospatial features one of multiple approaches, and saved in a geospatial data format (GeoPackage and GeoJSON). Features described using OSM's [nodes](https://wiki.openstreetmap.org/wiki/Node) or [ways](https://wiki.openstreetmap.org/wiki/Way) - typically points, lines, and simple polygons - are scrapped directly from OSM URLs by extracting coordinates. More complex features such as mulitpolygons represented by OSM's [relations](https://wiki.openstreetmap.org/wiki/Relation) are retrieved using the Overpass API. Finally, OSM URLs containing custom driving directions between two points utilize the Selenium webdriver to extract [SVG path](https://www.w3.org/TR/SVG/paths.html) details from map tiles which are then converted to geospatial features.

While the code developed has considerable error handling built in, it is still possibly to encounter scenarios where APIs are overloaded, web pages are down, or other edge cases that result in errors. Most errors will still be handled to avoid processing failures, and saved in dedicated outputs detailing the projects and error messages involved.

Note: This repository has only been tested on recent Ubuntu based Linux and MacOS systems


### Setup Environment:

1. Clone (or download) repository

Example:
```
git clone git@github.com:aiddata/gcdf-geospatial-data.git
cd gcdf-geospatial-data
```

2. Setup Python environment:

For the easiest setup, we strongly suggest using Conda and following the steps for one of the 3 options described below.

Notes before you start:
- If you do not already have Anaconda/Miniconda installed, please see their [installation guides](https://conda.io/projects/conda/en/latest/user-guide/install/index.html).
- Using Miniconda instead of the full Anaconda will be significant quicker to download/install.


- Option #1 (exact environment replication)
    - First run `conda env create -f environment.yml`
    - This will attempt to exactly replicate the environment we used to build the dataset. The caveat is that this may not always install cleanly on different operating systems. Modern Debian based Linux distributions are most likely to succeed.
    - Then run `pip install git+https://github.com/jacobwhall/osm2geojson.git@seth_debug`


- Option #2 (auto build based on core dependencies)
    - First run `conda env create -f core_environment.yml`
    - This will attempt to install only the direct dependencies required and allow additional dependencies to be determined by Conda. This has helped facilitate installs on MacOS systems, but has the potential to result in other dependency conflicts that could need to be resolved (due to the additional dependency choices)
    - Then run `pip install git+https://github.com/jacobwhall/osm2geojson.git@seth_debug`

- Option #3 (manual build based on core dependencies)
    - Run
        ```
        conda create -n geogcdf python=3.8
        conda activate geogcdf
        conda install -c conda-forge bs4 shapely pandas geopandas selenium==4.8.3 openpyxl conda-build
        pip install overpass
        pip install git+https://github.com/jacobwhall/osm2geojson.git@seth_debug
        pip install prefect==2.2.0 prefect-dask==0.1.2 bokeh>=2.1.1
        ```
    - This gives you the most step-by-step manual control in case you are running into dependency based installation issues. It is also the most flexible as we limit which packages are explicitly required by version. As a result, there is a greater chance of changes within dependencies resulting in errors running the code or producing slightly different outputs.


Finally, add the path to where you cloned the repo to your Conda environment:
`conda develop /path/to/gcdf-geospatial-data`

This may not be necessary, but can potentially prevent connection errors within Prefect:
`prefect config set PREFECT_API_ENABLE_HTTP2=false`

2. Install Firefox binary and geckodriver locally.

**On Linux:**
```
wget https://ftp.mozilla.org/pub/firefox/releases/104.0/linux-x86_64/en-US/firefox-104.0.tar.bz2
tar -xvf firefox-104.0.tar.bz2

wget https://github.com/mozilla/geckodriver/releases/download/v0.31.0/geckodriver-v0.31.0-linux64.tar.gz
tar -xvf geckodriver-v0.31.0-linux64.tar.gz
```

**On MacOS:**

Please note that you may need to download a [different build of geckodriver](https://github.com/mozilla/geckodriver/releases/) if you use an ARM Mac.
```
curl https://ftp.mozilla.org/pub/firefox/releases/104.0/mac/en-US/Firefox%20104.0.dmg -o Firefox-104.0.dmg
# Install Firefox from the downloaded .dmg

curl https://github.com/mozilla/geckodriver/releases/download/v0.31.0/geckodriver-v0.31.0-macos.tar.gz -o geckodriver-v0.31.0-macos.tar.gz
tar -xvf geckodriver-v0.31.0-macos.tar.gz
```

Notes:
- Varying builds of Firefox and Geckodriver may perform differently or have issues. This has only been run on Linux and was originally tested with Firefox 90.0.2 and Geckodriver 0.29.1, and has since been updated to Firefox 104.0 and Geckdriver 0.31.0.
- If you are running on a local machine with Firefox (or Chrome) already installed, you may opt to use that instead of downloading and installing a new copy. This will require modifying the code and as noted above, other versions have not been tested.
- On MacOS you will likely need to allow Geckodriver to run: see [this guide](https://stackoverflow.com/a/67205039)
- If you wish to use your existing system install of Firefox, you will need to link your system Firefox bin to the project directory. Here are commands that you may have to tweak depending on where Firefox is installed:
    - Linux: `ln -s /usr/lib/firefox/firefox-bin ./firefox/firefox-bin`
    - MacOS: `ln -s /Applications/Firefox.app/Contents/MacOS/firefox-bin ./firefox/firefox-bin`

You also have the option to install chromedriver with your own system install of Chrome
```
wget https://chromedriver.storage.googleapis.com/92.0.4515.43/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
```
Notes:
- If you are using Chrome you will need to edit the Python code to initiate the Selenium webdriver with Chrome instead of Firefox.
    - Some components to do this are commented out in the code, but they are likely not complete and not tested.
- The geckodriver and Firefox/Chrome install may need permissions adjusted after initial install

Potential issues:
- Sometimes openssl will not load correctly. First, try [this](https://stackoverflow.com/a/54389947), which will rebuild Python's cryptography package for your system and hopefully link to the correct openssl library. On MacOS, we've run into a similar "Library not loaded" error regarding openssl. To fix that issue, try the following steps:
    - [install brew](https://brew.sh/)
    - `brew install openssl@1.1`
    - `ln -s /usr/local/opt/openssl/lib/libssl.1.1.dylib /usr/local/lib`
    - `ln -s /usr/local/opt/openssl/lib/libcrypto.1.1.dylib /usr/local/lib`

3. Adjust variables

- Edit the `[main]` section of the  `config.ini` file to modify variables as needed:
    - `china-osm-geodata`: Should always be set to True
    - `base_directory`: path to your working directory (`/path/to/gcdf-geospatial-data`)
    - `active_run_name`: defines which run-specific section of the config.ini file to use.
    - `github_name`: GitHub owner of repo to push output data to
    - `github_repo`: name of GitHub repo to push output data to
    - `github_branch`: branch of GitHub repo to push output data to
    - `max_workers`: the maximum number of workers to use for parallel processing
    - `dask_enabled`: whether to use Dask for processing data
    - `dask_distributed`: whether to use a specific Dask cluster (if false, creates a local Dask cluster)
    - `dask_address`: the address of the Dask cluster to use if `dask_distributed` is True
    - `non_dask_serial`: if not using Dask, whether to run tasks in Serial (may help when testing) or Concurrently when possible

- Edit the run-specific section of the  `config.ini` file to modify variables as needed. You may have many run-specific section for different datasets or processing purposes, but the active one must be defined in the `active_run_name` field of the `[main]` config section.

    - `release_name`: a unique name that matches the directory within `./input_data` where input data is located, and is also used to create a corresponding directory in `./output_data` for outputs from processing.
    - `input_file_name`: the basename of the CSV or Excel file from which project level data containing OSM links and other information will be drawn
    - `sample_size` (bool): Number of samples of each OSM type (node, way, relation, directions) to be used for testing. To use all data, set to `-1`.
    - `use_existing_svg` (bool): Whether to use already processed information on OSM "directions" links from a previous run
    - `use_existing_feature` (bool): Whether to use already processed OSM features from a previous run
    - `use_existing_raw_osm` (bool): Whether to use raw OSM features retrieved/cached from a previous run
    - `existing_timestamp`: When either `use_existing_` variables above is set to `True`, this is the timestamp of an existing run from which already processed data will be pulled
    - `use_only_existing` (bool): Intended to be used when rerunning existing data and you do not wish to add any additional data from project files. This may apply when project files are updated, or more commonly when using a sampled subset for consistent testing.
    - `build_missing_cache` (bool): Whether to retrieve and cache any features not available in the existing cache from a previous run.
    - `prepare_only`: Boolean value indicating whether only the preliminary stage of data preparation will be run. See details in section below for use cases.
    - `id_field`, `location_field`, `precision_field`, `osm_str`, `invalid_str_list`: These are static variables that should not be changed for replication, yet are made available to support adapting this code for additional datasets in the future. `id_field` is a unique ID field in the input data; `location_field` is the field containing OSM links; `precision_field` described the precision level of the OSM feature relative to the true project feature; `osm_str` is the string used to identify OSM links; `invalid_str_list` is a list of strings associated with links that should be omitted from processing.
    - `output_project_fields` (list): the columns from the input data to be included in the output GeoJSONs as feature properties


### Setup Prefect UI and Dask cluster (optional)

1. Review `prefect_dask_setup.sh` and adjust if needed (most will not need to)
    - Make sure you are currently in your `gcdf-geospatial-data` directory in a terminal
    - Run `bash prefect_dask_setup.sh`
    - Notes:
        - If you run into issues when running with Prefect along the lines of `alembic.util.exc.CommandError: Can't locate revision identified by 'e757138e954a'` then you likely need to clear out your existing prefect database and data in `/path/to/gcdf-geospatial-data/.prefect/orion.db`. This is often the result of using different versionf of Prefect on your system.

### Run Code

1. Run the Python script
    - `python main.py`
