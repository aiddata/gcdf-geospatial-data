# Port Project Data Filtering and Exploration

Projects were identified based on keyword search of project titles related to ports. Additional projects were filtered based on sector (removing health, education, etc). There may still be a few erroneous projects included.

Projects were limited to Central/South America based on a simple longitude filter (any project west of -30 degrees included).


## Data

### All port related geocoded data
See `related.gpkg`. 20 total projects related to ports based on automated keyword search were found in the GeoGCDF v3.

### Precise Geocoded Data
See `specific.gpkg`. Contains 7 projects were identified after manual review as associated with port construction.

### Project data
See `project_level_data/projects.csv`. Contains related subset of project level data from the GCDF v3.

### ADM level aggregates
See `project_level_data/adm1.csv` and `project_level_data/adm1.csv`. Contains ADM1 and ADM2 level aggregates of spatial data based on related project commitment values.
