
# GCDF v3 Processing steps


## Prepare input data
`python input_data/gcdf_v3/gcdf_v3_prep.py`

## Run main processing
`python src/main.py`

**Note the timestamp of the main processing run, as all subsequent scripts will need to be updated with this timestamp to use the correct output data**


## Prepare data to be joined into project level GCDF v3 dataset

`python scripts/generate_project_join.py`


## Build ADM1 and ADM2 files

`python scripts/adm_lookup.py`


## Generate Basic Stats

`python stats/stats.py`


## Generate ESG Stats

First run the individual dataset extractions:
`python esg/critical_habitats/extract.py`
`python esg/protected_areas/extract.py`
`python esg/indigenous_lands/extract.py`
`python esg/PLAD/main.py`
Then combine into a single output:
`python esg/gcdf_v3_combine_outputs.py`
