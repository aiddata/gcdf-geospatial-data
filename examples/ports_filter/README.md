# Port Project Data Filtering and Exploration

Projects were identified based on keyword search of project titles related to ports. Additional projects were filtered based on sector (removing health, education, etc). There may still be a few erroneous projects included.

Projects were limited to Central/South America based on a simple longitude filter (any project west of -30 degrees included).


## Data

### Port related geocoded data
See `ports.gpkg`. 20 total projects related to ports were found in the GeoGCDF v3.

### Precise Geocoded Data
See `precise.gpkg`. Contains 20 projects geocoded with precise/approximate locations (0 projects were geocoded with admin boundaries)

### High Value Geocoded Data
See `high_value.gpkg`. Contains 16 geocoded projects with value > 1 million USD2021.

### Project data
See `project_level_data/projects.csv`. Contains related subset of project level data from the GCDF v3.

### ADM level aggregates
See `project_level_data/adm1.csv` and `project_level_data/adm1.csv`. Contains ADM1 and ADM2 level aggregates of spatial data based on related project commitment values.
