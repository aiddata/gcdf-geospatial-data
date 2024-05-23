
# GCDF v3 Release Processing Steps


## Prepare input data
`python input_data/gcdf_v3/gcdf_v3_prep.py`

## Run main processing
`python src/main.py`

**Note the timestamp (e.g., 2023_12_04_13_25) of the main processing run, as all subsequent scripts will need to be updated with this timestamp to use the correct output data**

### To publish a release candidate:
1. Update run as latest `bash scripts/set_latest.sh gcdf_v3 2023_12_04_13_25`
2. Commit changes and push to development GitHub repo (gcdf-geospatial-data-rc)
3. Create a new release in the development GitHub repo
4. Upload the `all_combined_global.gpkg.zip` and `osm_geojsons/OSM_grouped.zip` files to the release assets

### To publish an official release:
1. Create a PR on GitHub from the development repo to the production repo (gcdf-geospatial-data)
2. Merge the PR
3. Create a new release in the production GitHub repo
4. Upload the `all_combined_global.gpkg.zip` and `osm_geojsons/OSM_grouped.zip` files to the release assets


## Build ADM1 and ADM2 files
1. Edit adm lookup timestamp in Python script if needed
2. Run `python scripts/adm_lookup.py`


## Companion products for project-level GCDF release

### Prepare data to be joined into project level GCDF v3 dataset

1. Run `python scripts/generate_project_join.py`


### Generate Basic Stats

1. Run `python stats/stats.py`


### Generate ESG Stats

1. First run the individual dataset extractions:
`python esg/critical_habitats/extract.py`
`python esg/protected_areas/extract.py`
`python esg/indigenous_lands/extract.py`
`python esg/PLAD/main.py`
2. Then combine into a single output:
`python esg/gcdf_v3_combine_outputs.py`
