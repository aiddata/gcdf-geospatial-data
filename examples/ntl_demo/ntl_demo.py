"""
Generate stats and visualizations of change in NTL around vector features provided


Requirements:

Python 3.8+
shapely
rasterio
rasterstats
geopandas
matplotlib


Description:

This script provides a example of how the OpenStreetMap features provided with AidData's
Chinese Finance dataset can be used to generate a set of visualizations and statistics.

We will use a small set of features associated with Chinese funded projects,
consisting of a tranmissions line connecting a dam / power generation site and
a power substation. The impact of these activities on the surrounding area will
be evaluating using nighttime lights (NTL) data within a 5km buffer of the project sites.
Based on the approximate implementation date of 2016/2017, we will evaluate trends
and levels of NTL between 2014-2017 and 2017-2020.

The script will produce a set of statistic interpretations of the impact of the project
on NTL levels, as well as visualizations of the change in NTL trends over time.


Notes:

- The Python packages utilized in this script at not included in the requirements
or environment setup process for replicating the dataset preparation documented in this
repository.
- Additional elements of this script (primarily data paths) will need to be adjusted
as well before using.
- The project features used in this example are from a subset of two projects which were
combined manually outside of this script using QGIS.
- The script requires VIIRS data. Please see https://github.com/aiddata/geo-datasets/tree/master/viirs_ntl for more information on how AidData downloads and prepares this data.


"""

from pathlib import Path
import math

# import fiona
import pyproj
from shapely.geometry import shape, box
from shapely.ops import cascaded_union
import rasterio
import rasterio.mask
import rasterstats as rs
import geopandas as gpd

from rasterio.plot import show
import matplotlib.pyplot as plt
import matplotlib.colors as clr


# -------------------------------------
# user variables

# root path of all files references
base_path = Path("/home/userw/Desktop/tuff_osm/examples/ntl_demo")

# file name of the vector file containing the features to explore
vector_file_name = "combined_63447_63448.geojson"

# buffer size in meters
buffer_size_m = 5000

# -------------------------------------

# define and create outputs directory
output_dir = base_path / "output" / vector_file_name.split(".")[0]
output_dir.mkdir(parents=True, exist_ok=True)

# load vector data
vector_path = base_path / vector_file_name

vector_gdf = gpd.read_file(vector_path)
buffer_gdf = vector_gdf.copy()


# calculate UTM zone
# https://apollomapping.com/blog/gtm-finding-a-utm-zone-number-easily
utm_zone = math.ceil((buffer_gdf.total_bounds[0] + 180) / 6.0)
utm_south = buffer_gdf.total_bounds[3] < 0

crs = pyproj.CRS.from_dict({'proj': 'utm', 'zone': utm_zone, 'south': utm_south})
utm_epsg = crs.to_authority()[1]

# reproject to UTM, buffer, and reproject back to WG84 (EPSG:4326)
buffer_gdf = buffer_gdf.set_crs(epsg="4326")
buffer_gdf = buffer_gdf.to_crs(epsg=utm_epsg)

buffer_gdf["geometry"] = buffer_gdf["geometry"].buffer(buffer_size_m)

buffer_gdf = buffer_gdf.to_crs(epsg="4326")

# combine all features into a single feature
buffer_combined_shp = cascaded_union(buffer_gdf.geometry)
buffer_combined_gs = gpd.GeoSeries(buffer_combined_shp)


# load viirs ntl raster data
raster_dir_path = base_path / "viirs_ntl"

viirs2020 = rasterio.open(raster_dir_path / "2020.tif")
viirs2017 = rasterio.open(raster_dir_path / "2017.tif")
viirs2014 = rasterio.open(raster_dir_path / "2014.tif")


# generate bounding box of final vector feature (within list) to be used for masking
bounding_box = [box(*buffer_combined_shp.bounds)]

# mask viirs data to the bounding box
# https://rasterio.readthedocs.io/en/latest/topics/masking-by-shapefile.html
# https://rasterio.readthedocs.io/en/latest/api/rasterio.mask.html
viirs2020_image, viirs2020_transform = rasterio.mask.mask(viirs2020, bounding_box, crop=True)
viirs2017_image, viirs2017_transform = rasterio.mask.mask(viirs2017, bounding_box, crop=True)
viirs2014_image, viirs2014_transform = rasterio.mask.mask(viirs2014, bounding_box, crop=True)

# generate differences of two time periods
diff_2020_2017 = viirs2020_image - viirs2017_image
diff_2017_2014 = viirs2017_image - viirs2014_image

# define raster data output meta
out_meta = viirs2020.meta.copy()
out_meta.update({"driver": "GTiff",
                 "height": viirs2020_image.shape[1],
                 "width": viirs2020_image.shape[2],
                 "transform": viirs2020_transform})

# create subdirectory for clipped rasters
clip_dir = output_dir / "viirs_ntl_clipped"
clip_dir.mkdir(parents=True, exist_ok=True)


# write clipped trend rasters to files
with rasterio.open(output_dir / "diff_2020_2017.tif", "w", **out_meta) as dest:
    dest.write(diff_2020_2017)

with rasterio.open(output_dir / "diff_2017_2014.tif", "w", **out_meta) as dest:
    dest.write(diff_2017_2014)


# generate trend stats

diff_2020_2017_stats = rs.zonal_stats(buffer_combined_shp, output_dir / "diff_2020_2017.tif", stats="mean max min median sum")
diff_2017_2014_stats = rs.zonal_stats(buffer_combined_shp, output_dir / "diff_2017_2014.tif", stats="mean max min median sum")

mean_increase = (diff_2020_2017_stats[0]["mean"] - diff_2017_2014_stats[0]["mean"]) / diff_2017_2014_stats[0]["mean"]
max_increase = (diff_2020_2017_stats[0]["max"] - diff_2017_2014_stats[0]["max"]) / diff_2017_2014_stats[0]["max"]


# write clipped single year rasters to files
with rasterio.open(clip_dir / "2020.tif", "w", **out_meta) as dest:
    dest.write(viirs2020_image)

with rasterio.open(clip_dir / "2017.tif", "w", **out_meta) as dest:
    dest.write(viirs2017_image)

with rasterio.open(clip_dir / "2014.tif", "w", **out_meta) as dest:
    dest.write(viirs2014_image)


# generate single year stats (if needed)

year2020_stats = rs.zonal_stats(buffer_combined_shp, clip_dir / "2020.tif", stats="mean max min median sum")
year2017_stats = rs.zonal_stats(buffer_combined_shp, clip_dir / "2017.tif", stats="mean max min median sum")
year2014_stats = rs.zonal_stats(buffer_combined_shp, clip_dir / "2014.tif", stats="mean max min median sum")

year_agg_diff_2020_2017 = year2020_stats[0]["sum"] - year2017_stats[0]["sum"]
year_agg_diff_2017_2014 = year2017_stats[0]["sum"] - year2014_stats[0]["sum"]

year_agg_diff_increase = (year_agg_diff_2020_2017 - year_agg_diff_2017_2014) / year_agg_diff_2017_2014



# plot and save trend (differences) with project features / and buffer
# https://rasterio.readthedocs.io/en/latest/api/rasterio.plot.html

# create color map for visualizing NTL change
# red to black to light blue
cmap = clr.LinearSegmentedColormap.from_list('custom', ['#ff0000','#000000','#00ffff'], N=1024)


fig, ax = plt.subplots(1)
show(diff_2020_2017, transform=viirs2020_transform, cmap=cmap, vmin=-2, vmax=2, title="Change in Nighttime Light Output \nwithin Project Area, 2017-2020", ax=ax)
vector_gdf.plot(ax=ax, color="fuchsia", edgecolor="fuchsia", linewidth=0.5)
buffer_combined_gs.plot(ax=ax, color="none", edgecolor="lime", linewidth=0.5)
# plt.show()
plt.savefig(output_dir / "2020_2017_diff.png")


fig, ax = plt.subplots(1)
show(diff_2017_2014, transform=viirs2020_transform, cmap=cmap, vmin=-2, vmax=2, title="Change in Nighttime Light Output \nwithin Project Area, 2014-2017", ax=ax)
vector_gdf.plot(ax=ax, color="fuchsia", edgecolor="fuchsia", linewidth=0.5)
buffer_combined_gs.plot(ax=ax, color="none", edgecolor="lime", linewidth=0.5)
# plt.show()
plt.savefig(output_dir / "2017_2014_diff.png")


