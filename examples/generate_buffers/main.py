"""
Create buffered version of project features


Uses polygon_splitter to split buffered feature crossing the antimeridian.
Source: https://towardsdatascience.com/around-the-world-in-80-lines-crossing-the-antimeridian-with-python-and-shapely-c87c9b6e1513

"""

import json
from multiprocessing import Pool
from zipfile import ZipFile

import geopandas as gpd
import numpy as np
from shapely import to_geojson
from shapely.geometry import MultiPolygon, shape

import polygon_splitter

# path to zipped geojson in GitHub repository
# assumed working directory is the same directory this file is in (examples/generate_buffers)
input_zip_path = "../../all_combined_global.gpkg.zip"

# read zipfile into memory
input_zip = ZipFile(input_zip_path)
input_zip.extract("all_combined_global.gpkg")

# load zipfile contents into geodataframe and set crs
gdf = gpd.GeoDataFrame.from_file("all_combined_global.gpkg")
gdf = gdf.set_crs(epsg=4326)

# optional step: simplify 3 outliers with very large geometries
big_geoms = [178, 56959, 695]
gdf.loc[gdf.id.isin(big_geoms), "geometry"] = gdf.loc[
    gdf.id.isin(big_geoms), "geometry"
].simplify(0.00001)


# convert to equal area projection for more accurate buffers
gdf = gdf.to_crs("+proj=eck4 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs")

# define buffer sizes to generate and format for output paths
buffer_sizes = [0, 500, 2500, 5000]
buffer_path_template = "all_combined_global_BUFFERm.gpkg"


def fix_antimeridian(geom):
    # deal with geometries that cross antimeridian
    return MultiPolygon(
        [
            shape(json.loads(i))
            for i in polygon_splitter.split_polygon(json.loads(to_geojson(geom)))
        ]
    )


def buffer_data(bs):
    print(bs, "meters")
    # create copy of original dataset to avoid overwriting original geometry
    buffer_gdf = gdf.copy()
    # buffer
    if bs > 0:
        buffer_gdf.geometry = buffer_gdf.buffer(bs)
    # convert back to wgs84
    buffer_gdf = buffer_gdf.to_crs(epsg=4326)

    # here's how to find geometries that cross the antimeridian
    # buffer_gdf[buffer_gdf.geometry.bounds.maxx == np.inf]

    # buffer_gdf[buffer_gdf.geometry.bounds.maxx == np.inf].apply(fix_antimeridian)

    # convert to multipolygon
    buffer_gdf.geometry = buffer_gdf.geometry.apply(
        lambda x: MultiPolygon([x]) if x.geom_type == "Polygon" else x
    )
    # save buffered geometry to file
    buffer_output_path = str(buffer_path_template).replace("BUFFER", str(bs))
    buffer_gdf.to_file(buffer_output_path, driver="GPKG")


# run generation of buffers for each size in parallel
if __name__ == "__main__":
    pool = Pool()
    pool.map(buffer_data, buffer_sizes)
