"""
Rasterize project geometries with values based on commitments equally distributed

Generate rasters for all projects, and sector subsets ("TRANSPORT AND STORAGE", "ENERGY", "INDUSTRY, MINING, CONSTRUCTION")

Only rasterize projects that have "Recommended For Aggregates" set to "Yes"
"""

import os
import json
from zipfile import ZipFile

import geopandas as gpd
import numpy as np
from shapely.geometry import box
from tqdm import tqdm

from utility import rasterize_geom, Grid

# field from input geojson which contains the value to be
# distributed over geometry during rasterization (must be numeric)
val_field = 'Amount (Constant USD2017)'
# field containing the project sector name
# used for filtering
sector_field = 'AidData Sector Name'
# sector names to use for filtering (values)
# and short names for output filenames (keys)
sector_list = {
    # "all": "all",
    "transport": "TRANSPORT AND STORAGE",
    "energy": "ENERGY",
    "industry": "INDUSTRY, MINING, CONSTRUCTION"
}


# path to zipped geojson in GitHub repository
# assumed working directory is the same directory this file is in (examples/generate_buffers)
input_zip_path = "../../latest/development_combined_global.geojson.zip"

# read zipfile into memory
input_zip = ZipFile(input_zip_path)
geojson_bytes = input_zip.read("development_combined_global.geojson")

# load zipfile contents into geodataframe and set crs
geojson_dict = json.loads(geojson_bytes)
gdf = gpd.GeoDataFrame.from_features(geojson_dict)
gdf = gdf.set_crs(epsg=4326)

# ignore any projects not yet completed
gdf = gdf.loc[gdf["Recommended For Aggregates"] == "Yes"]

# optional step: simplify 3 outliers with very large geometries
big_geoms = [178, 56959, 695]
gdf.loc[gdf.id.isin(big_geoms), 'geometry'] = gdf.loc[gdf.id.isin(big_geoms), 'geometry'].simplify(0.00001)

# drop projects with no data for val_field
gdf_valid = gdf.loc[gdf[val_field] > 0].copy()

print(f"Total project count with valid commitment values: {len(gdf_valid)}")
print(f"Project counts for specified sectors: \n{gdf_valid.loc[gdf_valid[sector_field].isin(sector_list.values()), sector_field].value_counts()}")

# create dataframe with only necessary columns
gdf_rasterize = gdf_valid[['id', 'geometry', sector_field, val_field]].copy()


# -----------------
# if reload of utility imports are needed
# import utility
# from importlib import reload
# utility = reload(utility)
# Grid = utility.Grid
# rasterize_geom = utility.rasterize_geom
# -----------------
# for testing subsets of data
# gdf_tmp = gdf_rasterize[0:100].copy()
# gdf_tmp.to_file("tmp.geojson", driver="GeoJSON")
# gdf_tmp = gpd.read_file("tmp.geojson")
# -----------------

# output directory for rasterizations
os.makedirs("rasters", exist_ok=True)

pixel_size = 0.001

# geometry which defines the full extent of the final raster
full_geom = box(-180, -90, 180, 90)

# iterate over all sectors to be rasterized
for sector_name, sector_value in sector_list.items():
    if sector_name == "all":
        gdf_tmp = gdf_rasterize.copy()
    else:
        gdf_tmp = gdf_rasterize.loc[gdf_rasterize[sector_field] == sector_value].copy()

    # initialize grid to manage rasterized data
    grid = Grid(full_geom, pixel_size)

    # list of geometry objects to rasterize
    work_list = list(zip(gdf_tmp.id, gdf_tmp.geometry, gdf_tmp[val_field]))

    for id, geom, val in tqdm(work_list):
        print(id)
        surf, bounds = rasterize_geom(geom, grid.pixel_size)
        # determine surface values based on val_field value for project
        # distribution during rasterization
        adj_surf = surf * int(np.floor(val / surf.sum()))
        grid.update(bounds, adj_surf)

    # finalize the rasterization geotiff
    grid.save_geotiff(f"rasters/{sector_name}.tif")
