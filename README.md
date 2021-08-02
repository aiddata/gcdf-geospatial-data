# China OSM Geodata

Description of repo, China finance data, links to AidData, etc.


## Non-technical Data Description

Information about data, structure, etc

Links to data usage guides, examples, related material



## Technical Description

code, data, etc.


## Contributing

guidelines, etc



### Setup Environment:

1. Setup Conda Python environment:

```
conda create -n tuff_osm python=3.8
conda activate tuff_osm
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
    - Warning: if you do not use parallel processing this can take a significant amount of time to run for the full China dataset.


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