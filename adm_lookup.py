

import requests
from pathlib import Path

import geopandas as gpd

dataset_path = "/home/userx/Desktop/tuff_osm/output_data/3.0test20182020/results/2023_06_07_15_56/all_combined_global.geojson"
china_gdf = gpd.read_file(dataset_path)


adm_data_dir = Path("/home/userx/Desktop/gb_v5")
adm_data_dir.mkdir(parents=True, exist_ok=True)

# gb v5 release
adm1_cgaz_path = f"https://github.com/wmgeolab/geoBoundaries/raw/b7dd6a55701c76a330500ad9d9240f2b9997c6a8/releaseData/CGAZ/geoBoundariesCGAZ_ADM1.gpkg"
adm2_cgaz_path = f"https://github.com/wmgeolab/geoBoundaries/raw/b7dd6a55701c76a330500ad9d9240f2b9997c6a8/releaseData/CGAZ/geoBoundariesCGAZ_ADM2.gpkg"

adm1_dst_path = adm_data_dir / f"geoBoundariesCGAZ_ADM1.gpkg"
adm2_dst_path = adm_data_dir / f"geoBoundariesCGAZ_ADM2.gpkg"

def download_file(url, dst):
    r = requests.get(url)
    with open(dst, 'wb') as f:
        f.write(r.content)

if not adm1_dst_path.exists():
    download_file(adm1_cgaz_path, adm1_dst_path)
if not adm2_dst_path.exists():
    download_file(adm2_cgaz_path, adm2_dst_path)

adm1_gdf = gpd.read_file(adm1_dst_path, driver='GPKG')
adm1_gdf.geometry = adm1_gdf.geometry.buffer(0)

china_gdf["adm1_id"] = china_gdf["geometry"].apply(lambda x: adm1_gdf[adm1_gdf.intersects(x)].shapeID.to_list())
china_adm1_gdf = china_gdf.explode("adm1_id")
china_adm1_gdf.rename(columns={"adm1_id": "shapeID"}, inplace=True)
china_adm1_gdf = china_adm1_gdf[["id", "geometry", "shapeID"]].copy()
china_adm1_gdf = china_adm1_gdf.merge(adm1_gdf, on="shapeID", suffixes=("", "_adm1"))
china_adm1_gdf["intersection_ratio"] = china_adm1_gdf["geometry"].intersection(china_adm1_gdf["geometry_adm1"]).area / china_adm1_gdf["geometry"].area
china_adm1_gdf = china_adm1_gdf[["id", "shapeID", "shapeGroup", "shapeName", "geometry_adm1", "intersection_ratio"]].copy()
china_adm1_gdf.rename(columns={"geometry_adm1": "geometry"}, inplace=True)
china_adm1_gdf = gpd.GeoDataFrame(china_adm1_gdf, geometry="geometry", crs="EPSG:4326")
china_adm1_gdf.to_file(adm_data_dir / "china_adm1.gpkg", driver="GPKG")


adm2_gdf = gpd.read_file(adm2_dst_path, driver='GPKG')
adm2_gdf.geometry = adm2_gdf.geometry.buffer(0)

china_gdf["adm2_id"] = china_gdf["geometry"].apply(lambda x: adm2_gdf[adm2_gdf.intersects(x)].shapeID.to_list())
china_adm2_gdf = china_gdf.explode("adm2_id")
china_adm2_gdf.rename(columns={"adm2_id": "shapeID"}, inplace=True)
china_adm2_gdf = china_adm2_gdf[["id", "geometry", "shapeID"]].copy()
china_adm2_gdf = china_adm2_gdf.merge(adm2_gdf, on="shapeID", suffixes=("", "_adm2"))
china_adm2_gdf["intersection_ratio"] = china_adm2_gdf["geometry"].intersection(china_adm2_gdf["geometry_adm2"]).area / china_adm2_gdf["geometry"].area
china_adm2_gdf = china_adm2_gdf[["id", "shapeID", "shapeGroup", "shapeName", "geometry_adm2", "intersection_ratio"]].copy()
china_adm2_gdf.rename(columns={"geometry_adm2": "geometry"}, inplace=True)
china_adm2_gdf = gpd.GeoDataFrame(china_adm2_gdf, geometry="geometry", crs="EPSG:4326")
china_adm2_gdf.to_file(adm_data_dir / "china_adm2.gpkg", driver="GPKG")
