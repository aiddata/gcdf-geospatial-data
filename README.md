# China OSM Geodata

Description of repo, China finance data, links to [AidData](https://www.aiddata.org/), etc.

To download the latest data, click [HERE](https://github.com/aiddata/china-osm-geodata/blob/master/README.md) -- placeholder link

**AidData's Global Chinese Development Finance Dataset, Version 2.0**:
The 2.0 version of AidData’s Global Chinese Development Finance Dataset records the known universe of projects (with development, commercial, or representational intent) supported by official financial and in-kind commitments (or pledges) from China between 2000 and 2017, with implementation details covering a 22-year period (2000-2021). AidData systematically collected and quality-assured all projects in the dataset using the 2.0 version of the TUFF methodology.

**AidData's Global Chinese Military Finance Dataset** contains data on projects backed by financial and in-kind commitments (or pledges) from official sources in China that were provided with military intent.  AidData's Global Chinese Development Finance Dataset (Version 2.0) captures military-related projects supported by official commitments (and pledges) from China that are consistent with OECD-DAC criteria for Official Development Assistance (ODA).  These include any projects with development intent that involve the military and exclude projects supported by official commitments (and pledges) that seek to promote the security interests of the country from which the financial transfer originates (the People’s Republic of China) or strengthen the capabilities of military institutions in the recipient country.  However, given that some users have expressed interest in learning more about projects with military intent that are backed by official financial and kind transfers from China, AidData has recorded these “ODA-adjacent” and “OOF-adjacent” projects in a separate tab of the data file. This tab includes 390 projects worth $9.9 billion in 89 countries that have military intent and are backed by financial and in-kind commitments (or pledges) from official sources in China (e.g., the Ministry of Defense, the People’s Liberation Army, and military state-owned enterprises like NORINCO and Poly Technologies). These projects were committed (or pledged) over an 18-year period (2000- 2017), with implementation details covering a 22-year period (2000-2021).  AidData systematically collected and quality-assured all projects in this dataset using the 2.0 version of the TUFF methodology. However, under OECD-DAC guidelines, they do not clearly qualify as ODA or OOF.

**AidData's Global Huawei Finance Dataset** contains data on projects backed by financial and in-kind commitments (and pledges) from Huawei Technologies Co., Ltd. (“Huawei”) and its subsidiaries. AidData's Global Chinese Development Finance Dataset (Version 2.0) includes projects supported by official commitments (and pledges) from China that were implemented by Huawei and its subsidiaries. However, due to uncertainty about whether Huawei should be treated as a private company or state-owned company, it excludes projects backed by financial and in-kind commitments (and pledges) from Huawei and its subsidiaries. At the same time, some users have expressed interest in learning more about Huawei-financed projects, so AidData has recorded these “ODA-adjacent” and “OOF-adjacent” projects in a separate tab in of the data file. This tab includes 152 projects worth $1.7 billion in 63 countries. These projects were backed by financial and in-kind commitments (and pledges) over an 18-year period (2000- 2017), with implementation details covering a 22-year period (2000-2021).  AidData systematically collected and quality-assured all projects in this dataset using the 2.0 version of the TUFF methodology. However, under OECD-DAC guidelines, they do not clearly qualify as ODA or OOF.


## Description

Information about data, structure, etc

Links to data usage guides, examples, related material


The original dataset produced by AidData is licensed using the [Open Data Commons Attribution License (ODC-By)](https://opendatacommons.org/licenses/by/1-0/). Geospatial data extracted from OpenStreeMaps (OSM) is licensed using the [Open Data Commons Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/1-0/).

Any use of the dataset and its content must include appropriate attribution to AidData, while uses incorporating the geospatial features from OSM must also attribute OSM and be made available using ODbL.

Please see the [LICENSE file](LICENSE.md) for human-readable summaries of both ODC-By and ODbL, and links to the full licenses.


The following citation may be used to credit AidData:
```
TBD
```

OpenStreetMap may be credited using `© OpenStreetMap contributors` or by linking to their copyright page ([https://www.openstreetmap.org/copyright](https://www.openstreetmap.org/copyright))


## Contributing

Whether you have questions about usage, discover a bug, or just want to engage with others using the data, there is a place for you!

[GitHub Discussions](https://github.com/aiddata/china-osm-geodata/discussions) are a great place to ask questions, see what others are talking about, and share research, applications, and ideas. If you'd prefer to get involved in more technical aspects, want to suggest improvements, discover a bug in the code, or run into issues with the data, then please utilize [GitHub's Issues](https://github.com/aiddata/china-osm-geodata/issues).



## Replication

At AidData we believe in transparency and making our work replicable. In this section we provide all the steps necessary to replicate the complete workflow used to produce the data in this repository.


### Technical Overview

The code in this repository utilizes a combination of webscrapping, APIs, and geospatial processing to extract geospatial features from OpenStreeMap (OSM) URLs which detail either OSM features or driving directions. Python is used for all primary processing (Shell scripts are also used to initialize parallel jobs and manage repository files) and has been tested with packages and dependencies managed by Anaconda (see section on setting up environment below). While all code can be run on a local environment using a single process, leveraging parallel processing (implemented using [mpi4py](https://github.com/mpi4py/mpi4py) on [William & Mary's HPC](https://www.wm.edu/offices/it/services/researchcomputing/atwm/index.php)) will substantially reduce the amount of time needed to run.

Python webscrapping is dependent on [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) and [Selenium](https://selenium-python.readthedocs.io/), and access to the OSM Overpass API leverages the [Overpass API Python wrapper](https://github.com/mvexel/overpass-api-python-wrapper). Additional geospatial processing and data management utilizes [Shapely](https://shapely.readthedocs.io/en/stable/manual.html), [osm2geojson](https://github.com/aspectumapp/osm2geojson), and [Pandas](https://pandas.pydata.org/).

Project data prepared using AidData's TUFF (Tracking Underreport Financial Flows) methodology serves an input and contains text descriptions, incuding OSM URLs, of locations associated with Chinese financed projects which are identified by a unique "AidData Tuff Project ID". For each project, OSM URLs are extracted (projects may have zero or multiple OSM URLs) and converted into geospatial features one of multiple approaches, and saved as a GeoJSON. Features described using OSM's [nodes](https://wiki.openstreetmap.org/wiki/Node) or [ways](https://wiki.openstreetmap.org/wiki/Way) - typically points, lines, and simple polygons - are scrapped directly from OSM URLs by extracting coordinates. More complex features such as mulitpolygons represented by OSM's [relations](https://wiki.openstreetmap.org/wiki/Relation)are retrieved using the Overpass API. Finally, OSM URLs containing custom driving directions between two points utilize the Selenium webdriver to extract [SVG path](https://www.w3.org/TR/SVG/paths.html) details from map tiles which are then converted to geospatial features.


### Setup Environment:

1. Clone (or download) repository

Example:
```
git clone git@github.com:aiddata/china-osm-geodata.git
cd china-osm-geodata
```

2. Setup Python environment:

For the easiest setup, we strongly suggest using Anaconda and following the steps below. If you do not already have Anaconda installed, please see their [installation guides](https://docs.anaconda.com/anaconda/install/index.html).

When using Conda, you will typically want to unset the `PYTHONPATH` variable, e.g. `unsetenv PYTHONPATH` when using tcsh or `unset PYTHONPATH` for bash

```
conda create -n china_osm python=3.8
conda activate china_osm
conda install -c conda-forge bs4 shapely pandas selenium==3.141.0
pip install osm2geojson==0.1.29 overpass
# Only required if using parallel processing
pip install mpi4py
```

Notes:
- The `environment.yml` file is available with specific versions of packages installed by Conda if needed (specific Selenium version specified above).
    - Use `conda env create -f environment.yml` to builds from this file.
    - You will still need to install packages using pip after creating the environment.
- pip was needed to install osm2geojson in order to get newer version (may be available through latest conda, but not tested in our build)
- If parallel processing, pip is needed to install mpi4py based on system build of mpi (e.g., OpenMPI)
    - Warning: if you do not use parallel processing this can take a significant amount of time to run for the full dataset.


2. Install FireFox binary and geckodriver locally.

```
wget https://ftp.mozilla.org/pub/firefox/releases/90.0.2/linux-x86_64/en-US/firefox-90.0.2.tar.bz2
tar -xvf firefox-90.0.2.tar.bz2

wget https://github.com/mozilla/geckodriver/releases/download/v0.29.1/geckodriver-v0.29.1-linux64.tar.gz
tar -xvf geckodriver-v0.29.1-linux64.tar.gz
```
Notes:
- Varying builds of FireFox and Geckodriver may perform differently or have issues. This build has only been tested with Firefox 90.0.2 and Geckodriver 0.29.1.
- If you are running on a local machine with FireFox (or Chrome) already installed, you may opt to use that instead of downloading and installing a new copy. This will require modifying the code and as noted above, other versions have not been tested.


You also have the option to install chromedriver with your own system install of Chrome
```
wget https://chromedriver.storage.googleapis.com/92.0.4515.43/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
```
Notes:
- If you are using Chrome you will need to edit the Python code to initiate the Selenium webdriver with Chrome instead of Firefox.
    - Some components to do this are commented out in the code, but they are not likely complete and not tested.


3. Adjust variables

- Edit the `config.ini` file to modify variables as needed
    - `mode`: set to `parallel` or `serial` depending on how you intend to run the processing (Note: if you do not have an mpi4py installation, the parallel processing option is unlikely to work properly)
    - `max_workers`: the maximum number of workers to use for parallel processing
    - `release_name`: a unique name that matches the directory within `./input_data` where input data is located, and is also used to create a corresponding directory in `./output_data` for outputs from processing.
    - `from_existing`: Primarily used for debugging/testing with new data. Boolean value indicating whether preliminary data from a previous run should be used to instantiate the current run.
    - `from_existing_timestamp`: When `from_existing` is set to `True`, this is the timestamp used to create the directory of the output data (e.g., `./output_data/<release_name>/results/<timestamp>`) that will be used to instantiate the current run
    - `prepare_only`: Boolean value indicating whether only the preliminary stage of data preparation will be run. See details in section below for use cases.


- Note: you should not need to adjust the `release_name` when replicating the official processed data


## Run Code

1. Run the Python script
    - In serial:
        - `python tuff_osm.py`
    - In parallel on W&M's HPC:
        - Edit `jobscript` to set the source directory for files on the HPC
        - `qsub jobscript`

2. (Optional) The portion of the script which extracts information from OSM links which are driving directions is the slowest portion as it much be run in serial (for now). As a result, if you intend to use parallel processing in general you may wish to run this portion separately first, then run the remaining code in parallel.
    - To support this, you may set the `prepare_only` config option to `True` and the script will exit after the initial data preparation stage which includes acquiring data on OSM driving directions.
    - After using the `prepare_only = True` and `mode = serial` to run the script, you may set `prepare_only = False` and `mode = parallel` and use the accompanying `from_existing` options described above to run the second stage of processing.