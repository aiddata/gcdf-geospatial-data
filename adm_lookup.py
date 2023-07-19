

import requests
from pathlib import Path

import geopandas as gpd


dry_run = True
intersection_threshold = 0.0001


dataset_path = "/home/userx/Desktop/tuff_osm/output_data/3.0test2021/results/2023_07_11_16_53/all_combined_global.geojson"
value_field = "Amount (Constant USD2021)"
china_gdf = gpd.read_file(dataset_path)

# china_gdf = china_gdf.loc[china_gdf.id ==97577]

adm_data_dir = Path("/home/userx/Desktop/tuff_osm/gb_v5")
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
adm1_gdf = adm1_gdf.loc[adm1_gdf.shapeID.notnull()].copy()

china_gdf["adm1_id"] = china_gdf["geometry"].apply(lambda x: adm1_gdf[adm1_gdf.intersects(x)].shapeID.to_list())
china_adm1_gdf = china_gdf.explode("adm1_id")
china_adm1_gdf.rename(columns={"adm1_id": "shapeID"}, inplace=True)
china_adm1_gdf = china_adm1_gdf[["id", value_field, "geometry", "shapeID"]].copy()
china_adm1_gdf = china_adm1_gdf.merge(adm1_gdf, on="shapeID", suffixes=("", "_adm1"))
china_adm1_gdf["intersection_ratio"] = china_adm1_gdf["geometry"].intersection(china_adm1_gdf["geometry_adm1"]).area / china_adm1_gdf["geometry"].area

initial_adm1_count = china_adm1_gdf.shape[0]
china_adm1_gdf = china_adm1_gdf.loc[china_adm1_gdf["intersection_ratio"] > intersection_threshold].copy()
final_adm1_count = china_adm1_gdf.shape[0]
print(f"Removed {initial_adm1_count - final_adm1_count} adm1s (from {initial_adm1_count}) with intersection ratio < {intersection_threshold}. Final count is {final_adm1_count}.")

china_adm1_gdf = china_adm1_gdf[["id", value_field, "shapeID", "shapeGroup", "shapeName", "geometry_adm1", "intersection_ratio"]].copy()
china_adm1_gdf.rename(columns={"geometry_adm1": "geometry"}, inplace=True)
china_adm1_gdf = gpd.GeoDataFrame(china_adm1_gdf, geometry="geometry", crs="EPSG:4326")
china_adm1_gdf["centroid"] = china_adm1_gdf["geometry"].centroid
china_adm1_gdf["centroid_longitude"] = china_adm1_gdf["centroid"].x
china_adm1_gdf["centroid_latitude"] = china_adm1_gdf["centroid"].y
china_adm1_gdf.drop(columns=["centroid"], inplace=True)

project_adm1_counts = china_adm1_gdf.groupby("id").size().reset_index(name="adm_count")
project_adm1_counts["even_split_ratio"] = 1 / project_adm1_counts["adm_count"]
china_adm1_gdf = china_adm1_gdf.merge(project_adm1_counts[["id", "even_split_ratio"]], on="id")
china_adm1_gdf[value_field] = china_adm1_gdf[value_field].astype(float)
china_adm1_gdf["intersection_ratio_committment_value"] = china_adm1_gdf["intersection_ratio"] * china_adm1_gdf[value_field]
china_adm1_gdf["even_split_ratio_committment_value"] = china_adm1_gdf["even_split_ratio"] * china_adm1_gdf[value_field]
china_adm1_gdf.drop(columns=[value_field], inplace=True)

china_adm1_gdf.to_file(adm_data_dir / "china_adm1.gpkg", driver="GPKG")
china_adm1_gdf[[i for i in china_adm1_gdf.columns if i != "geometry"]].to_csv(adm_data_dir / "china_adm1.csv", index=False)


adm2_gdf = gpd.read_file(adm2_dst_path, driver='GPKG')
adm2_gdf.geometry = adm2_gdf.geometry.buffer(0)
adm2_gdf = adm2_gdf.loc[adm2_gdf.shapeID.notnull()].copy()

china_gdf["adm2_id"] = china_gdf["geometry"].apply(lambda x: adm2_gdf[adm2_gdf.intersects(x)].shapeID.to_list())
china_adm2_gdf = china_gdf.explode("adm2_id")
china_adm2_gdf.rename(columns={"adm2_id": "shapeID"}, inplace=True)
china_adm2_gdf = china_adm2_gdf[["id", value_field, "geometry", "shapeID"]].copy()
china_adm2_gdf = china_adm2_gdf.merge(adm2_gdf, on="shapeID", suffixes=("", "_adm2"))
china_adm2_gdf["intersection_ratio"] = china_adm2_gdf["geometry"].intersection(china_adm2_gdf["geometry_adm2"]).area / china_adm2_gdf["geometry"].area

initial_adm2_count = china_adm2_gdf.shape[0]
china_adm2_gdf = china_adm2_gdf.loc[china_adm2_gdf["intersection_ratio"] > intersection_threshold].copy()
final_adm2_count = china_adm2_gdf.shape[0]
print(f"Removed {initial_adm2_count - final_adm2_count} adm2s (from {initial_adm2_count}) with intersection ratio < {intersection_threshold}. Final count is {final_adm2_count}.")

china_adm2_gdf = china_adm2_gdf[["id", value_field, "shapeID", "shapeGroup", "shapeName", "geometry_adm2", "intersection_ratio"]].copy()
china_adm2_gdf.rename(columns={"geometry_adm2": "geometry"}, inplace=True)
china_adm2_gdf = gpd.GeoDataFrame(china_adm2_gdf, geometry="geometry", crs="EPSG:4326")
china_adm2_gdf["centroid"] = china_adm2_gdf["geometry"].centroid
china_adm2_gdf["centroid_longitude"] = china_adm2_gdf["centroid"].x
china_adm2_gdf["centroid_latitude"] = china_adm2_gdf["centroid"].y
china_adm2_gdf.drop(columns=["centroid"], inplace=True)

project_adm2_counts = china_adm2_gdf.groupby("id").size().reset_index(name="adm_count")
project_adm2_counts["even_split_ratio"] = 1 / project_adm2_counts["adm_count"]
china_adm2_gdf = china_adm2_gdf.merge(project_adm2_counts[["id", "even_split_ratio"]], on="id")
china_adm2_gdf[value_field] = china_adm2_gdf[value_field].astype(float)
china_adm2_gdf["intersection_ratio_committment_value"] = china_adm2_gdf["intersection_ratio"] * china_adm2_gdf[value_field]
china_adm2_gdf["even_split_ratio_committment_value"] = china_adm2_gdf["even_split_ratio"] * china_adm2_gdf[value_field]
china_adm2_gdf.drop(columns=[value_field], inplace=True)

china_adm2_gdf.to_file(adm_data_dir / "china_adm2.gpkg", driver="GPKG")
china_adm2_gdf[[i for i in china_adm2_gdf.columns if i != "geometry"]].to_csv(adm_data_dir / "china_adm2.csv", index=False)
