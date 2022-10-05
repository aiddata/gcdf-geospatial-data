# China OSM Geodata

Welcome to the GitHub repository for [AidData's](https://www.aiddata.org/) Global Chinese Development Finance Dataset! Please also visit AidData's [dedicated page for work related to China](https://www.aiddata.org/china), where you can find blog posts, announcements, and more.

This repository allows you to:
1. Download the main dataset as well as the accompanying geospatial features extract from [OpenStreetMap (OSM)](https://www.openstreetmap.org/)
2. Explore examples, guidelines, other material related to using the geospatial features
3. Access the code used to extract and process geospatial features from OSM in order to replicate the geospatial data we provide

<br/>

### [Download AidData’s Global Chinese Development Finance Dataset](https://www.aiddata.org/data/aiddatas-global-chinese-development-finance-dataset-version-2-0)


### [Download the accompanying geospatial dataset](https://github.com/aiddata/china-osm-geodata/raw/master/latest/development_combined_global.geojson.zip)


<br/>

## Dataset Description

The 2.0 version of **AidData’s Global _Chinese Development Finance_ Dataset** records the known universe of projects (with development, commercial, or representational intent) supported by official financial and in-kind commitments (or pledges) from China between 2000 and 2017, with implementation details covering a 22-year period (2000-2021). AidData systematically collected and quality-assured all projects in the dataset using the 2.0 version of the TUFF methodology.

In addition to Development Finance, separate datasets on Chinese Military Finance and Huawei Finance are provided. **AidData's Global _Chinese Military Finance_ Dataset** contains data on projects backed by financial and in-kind commitments (or pledges) from official sources in China that were provided with military intent. **AidData's Global _Huawei Finance_ Dataset** contains data on projects backed by financial and in-kind commitments (and pledges) from Huawei Technologies Co., Ltd. (“Huawei”) and its subsidiaries. Due to some military finance falling outside of the OECD-DAC criteria for Official Development Assistance and uncertainty about whether Huawei should be treated as a private company or state-owned company, these projects are provided as separate datasets. To read more about these datasets, please read [their full descriptions](input_data/2.0release/README.md).

For a subset of these projects, precise geographic features associated with the location of project activities were identifed through OpenStreetMap. Projects for which OSM features are available have links included in the main dataset to visualize or download the geospatial data from this GitHub repository. Geospatial data is available in the [GeoJSON](https://geojson.org/) format, and are saved in the [latest/geojsons](latest/geojsons) folder, with one GeoJSON per project named according to the project ID. For example, Project 35756 can be viewed via [latest/geojsons/35756.geojson](latest/geojsons/35756.geojson).

Combined GeoJSONS (within a Zip file) are also available within the [latest](latest) folder. The combined GeoJSONS are available for [Chinese Development Finance projects](latest/development_combined_global.geojson.zip), [Chinese Military Finance projects](latest/military_combined_global.geojson.zip), [Huawei Finance projects](latest/huawei_combined_global.geojson.zip), and [all projects](latest/all_combined_global.geojson.zip).


Examples, guides, best practices, and other material related to utilizing the geospatial data can be found in the [Examples](examples) folder. For information on using the core datset and project data, please explore the tabs of the official dataset (see download link for the latest data above).



<br/>

## License

The original dataset produced by AidData is licensed using the [Open Data Commons Attribution License (ODC-By)](https://opendatacommons.org/licenses/by/1-0/). Geospatial data extracted from OpenStreeMaps (OSM) is licensed using the [Open Data Commons Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/1-0/).

Any use of the dataset and its content must include appropriate attribution to AidData, while uses incorporating the geospatial features from OSM must also attribute OSM and be made available using ODbL.

Please see the [LICENSE file](LICENSE.md) for human-readable summaries of both ODC-By and ODbL, and links to the full licenses.


The following citations may be used to credit AidData (Please note: Both works count as the official citation for this dataset):
```
Custer, S., Dreher, A., Elston, T.B., Fuchs, A., Ghose, S., Lin, J., Malik, A., Parks, B.C., Russell, B., Solomon, K., Strange, A., Tierney, M.J., Walsh, K., Zaleski, L., and Zhang, S. 2021. Tracking Chinese Development Finance: An Application of AidData’s TUFF 2.0 Methodology. Williamsburg, VA: AidData at William & Mary.

Dreher, A., Fuchs, A., Parks, B. C., Strange, A., & Tierney, M.J. (Forthcoming). Banking on Beijing: The Aims and Impacts of China’s Overseas Development Program. Cambridge, UK: Cambridge University Press.

```

OpenStreetMap may be credited using `© OpenStreetMap contributors` or by linking to their copyright page ([https://www.openstreetmap.org/copyright](https://www.openstreetmap.org/copyright))


<br/>

## Contributing

Whether you have questions about usage, discover a bug, or just want to engage with others using the data, there is a place for you!

[GitHub Discussions](https://github.com/aiddata/china-osm-geodata/discussions) are a great place to ask questions, see what others are talking about, and share research, applications, and ideas. If you'd prefer to get involved in more technical aspects, want to suggest improvements, discover a bug in the code, or run into issues with the data, then please utilize [GitHub's Issues](https://github.com/aiddata/china-osm-geodata/issues).


<br/>

## Replication

At AidData we believe in transparency and making our work replicable. In this section we provide all the steps necessary to replicate the complete workflow used to produce the data in this repository.


### Technical Overview

The code in this repository utilizes a combination of webscrapping, APIs, and geospatial processing to extract geospatial features from OpenStreeMap (OSM) URLs which detail either OSM features or driving directions. Python is used for all primary processing (Shell scripts are also used to initialize parallel jobs and manage repository files) and has been tested with packages and dependencies managed by Anaconda (see section on setting up environment below). All code can be run on a local environment using a single process or by leveraging parallel processing (implemented using both [concurrent futures](https://docs.python.org/3/library/concurrent.futures.html), as well as [mpi4py](https://github.com/mpi4py/mpi4py) on [William & Mary's HPC](https://www.wm.edu/offices/it/services/researchcomputing/atwm/index.php)), which will substantially reduce the amount of time needed to run.

Python webscrapping is dependent on [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) and [Selenium](https://selenium-python.readthedocs.io/), and access to the OSM Overpass API leverages the [Overpass API Python wrapper](https://github.com/mvexel/overpass-api-python-wrapper). Additional geospatial processing and data management utilizes [Shapely](https://shapely.readthedocs.io/en/stable/manual.html), [osm2geojson](https://github.com/aspectumapp/osm2geojson), and [Pandas](https://pandas.pydata.org/).

Project data prepared using AidData's TUFF (Tracking Underreport Financial Flows) methodology serves an input and contains text descriptions, incuding OSM URLs, of locations associated with Chinese financed projects which are identified by a unique "AidData Tuff Project ID". For each project, OSM URLs are extracted (projects may have zero or multiple OSM URLs) and converted into geospatial features one of multiple approaches, and saved as a GeoJSON. Features described using OSM's [nodes](https://wiki.openstreetmap.org/wiki/Node) or [ways](https://wiki.openstreetmap.org/wiki/Way) - typically points, lines, and simple polygons - are scrapped directly from OSM URLs by extracting coordinates. More complex features such as mulitpolygons represented by OSM's [relations](https://wiki.openstreetmap.org/wiki/Relation)are retrieved using the Overpass API. Finally, OSM URLs containing custom driving directions between two points utilize the Selenium webdriver to extract [SVG path](https://www.w3.org/TR/SVG/paths.html) details from map tiles which are then converted to geospatial features.

While the code developed has considerable error handling built in, it is still possibly to encounter scenarios where APIs are overloaded, web pages are down, or other edge cases that result in errors. Most errors will still be handled to avoid processing failures, and saved in dedicated outputs detailing the projects and error messages involved.

Note: This repository has only been tested on recent Ubuntu based Linux and MacOS systems


### Setup Environment:

1. Clone (or download) repository

Example:
```
git clone git@github.com:aiddata/china-osm-geodata.git
cd china-osm-geodata
```

2. Setup Python environment:

For the easiest setup, we strongly suggest using Conda and following the steps below.
- If you do not already have Anaconda/Miniconda installed, please see their [installation guides](https://conda.io/projects/conda/en/latest/user-guide/install/index.html).
- Using Miniconda instead of the full Anaconda will be significant quicker to download/install.


```
conda create -n china_osm python=3.9
conda activate china_osm
conda install -c conda-forge bs4 shapely pandas geopandas selenium==3.141.0 openpyxl conda-build
pip install osm2geojson==0.1.29 overpass
pip install prefect==2.2.0 prefect-dask==0.1.2 bokeh>=2.1.1
```

Notes:
- The `core_requirements.yml` file is available with specific versions of core packages installed by Conda if needed (specific Selenium version specified above).
    - Use `conda env create -f core_requirements.yml` to builds from this file.
    - You may still need to install packages using pip after creating the environment in some cases.
- pip was needed to install osm2geojson and prefect in order to get newer versions (may be available through latest conda, but not tested in our build)
- Due to variability in dependency versions that will work across systems (e.g., Linux vs Mac) we suggest using the core_requirements to create your build rather than replicating our full test environment. If you do wish to replicate our test environmentally exactly (built for Ubuntu based Linux), you may use the `environment.yml` file instead.

Add the path to where you cloned the repo to your Conda environment:
`conda develop /path/to/china-osm-geodata`


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
    - `base_directory`: path to your working directory (`/path/to/china-osm-geodata`)
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
    - `sample_size` (bool): Number of samples of each OSM type (node, way, relation, directions) to be used for testing. To use all data, set to `-1`.
    - `use_existing_svg` (bool): Whether to use already processed information on OSM "directions" links from a previous run
    - `use_existing_feature` (bool): Whether to use already processed OSM features from a previous run
    - `from_existing_timestamp`: When either `use_existing_svg` or `use_existing_feature` is set to `True`, this is the timestamp of an existing run from which already processed data will be pulled
    - `use_only_existing` (bool): Intended to be used when rerunning existing data and you do not wish to add any additional data from project files. This may apply when project files are updated, or more commonly when using a sampled subset for consistent testing.
    - `prepare_only`: Boolean value indicating whether only the preliminary stage of data preparation will be run. See details in section below for use cases.
    - `id_field`, `location_field`, `precision_field`, and `osm_str`: These are static variables that should not be changed for replication, yet are made available to support adapting this code for additional datasets in the future. `id_field` is a unique ID field in the input data, `location_field` is the field containing OSM links, `precision_field` described the precision level of the OSM feature relative to the true project feature, and `osm_str` is the string used to identify OSM links.
    - `output_project_fields` (list): the columns from the input data to be included in the output GeoJSONs as feature properties


### Setup Prefect UI and Dask cluster (optional)

1. See `prefect_dask_setup.sh`
    - **To be updated**

### Run Code

1. Run the Python script
    - Locally:
        - `python tuff_osm.py`
    - On W&M's HPC:
        - **To be updated**
