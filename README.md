# China OSM Geodata

Description of repo, China finance data, links to AidData, etc.



## Non-technical Data Description

Information about data, structure, etc

Links to data usage guides, examples, related material



## Technical Description



## Setup:

conda create -n tuff_osm python=3.8
conda activate tuff_osm
conda install -c conda-forge bs4 shapely pandas selenium
pip install osm2geojson==0.1.29 overpass
pip install mpi4py

pip is needed to install osm2geojson in order to get newer version
pip is needed to install mpi4py based on system build of mpi (e.g., OpenMPI)

install geckodriver or chromedriver
```
wget https://github.com/mozilla/geckodriver/releases/download/v0.29.1/geckodriver-v0.29.1-linux64.tar.gz
tar -xvf geckodriver-v0.29.1-linux64.tar.gz

wget https://chromedriver.storage.googleapis.com/92.0.4515.43/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
```

Install FireFox binary
```
wget "https://download.mozilla.org/?product=firefox-latest&os=linux64&lang=en-US" -O latest-firefox.tar.bz2
```


To Do:
- Expand readme
- Finalize repo structure
- Improve "directions" feature extraction which currently relies on serial parsing of the map using Selenium (not thread safe). Can potentially rebuild routing API queries directly
- Add output from processing which can be used to notify when primary OSM feature from link was deleted and old version had to be used
- Move functions to separate class (Possibly make a generalized OSM extraction tool. This may not work well without hurting our ability to parallelize efficiently - will need to explore further once first version is stable.)
- Add unit tests for smaller, core functions (possibly break any larger, non-management, functions into smaller ones where practical)
- Add license?
- Add input data so results are fully replicable?


## Contributing

guidelines, etc
