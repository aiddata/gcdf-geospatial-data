

from pathlib import Path

import pandas as pd
import geopandas as gpd


output_tag = "gcdf_v3"
output_timestamp = "2023_10_18_09_35"

dataset_path = f"/home/userx/Desktop/tuff_osm/output_data/gcdf_v3/results/{output_timestamp}/all_combined_global.gpkg"
raw_china_gdf = gpd.read_file(dataset_path)

project_data_path = "/home/userx/Desktop/tuff_osm/input_data/gcdf_v3/cdf2021.csv"
project_df = pd.read_csv(project_data_path)
project_df = project_df[['AidData Tuff Project ID', 'Recipient ISO-3']]

adm_data_dir = Path("/home/userx/Desktop/tuff_osm/gb_v6")
adm1_list_path = adm_data_dir / f"{output_tag}_adm1.gpkg"
adm1_gdf = gpd.read_file(adm1_list_path, driver='GPKG')



adm1_iso_gdf1 = adm1_gdf.merge(project_df, left_on="id", right_on="AidData Tuff Project ID", how="left")

adm1_iso_gdf2 = adm1_iso_gdf1.copy()
# adm1_iso_gdf2 = adm1_iso_gdf1.drop_duplicates(subset=['id', 'shapeGroup', 'Recipient ISO-3']).copy()
adm1_iso_gdf2["matching_iso"] = adm1_iso_gdf2["Recipient ISO-3"] == adm1_iso_gdf2["shapeGroup"]

adm1_iso_gdf3 = adm1_iso_gdf2.groupby("id").agg({"matching_iso": list}).reset_index()
adm1_iso_gdf3.rename(columns={"matching_iso": "iso_match_list"}, inplace=True)


adm1_iso_gdf4 = adm1_iso_gdf2.merge(adm1_iso_gdf3, on="id", how="left")


adm1_iso_gdf5 = adm1_iso_gdf4.loc[adm1_iso_gdf4["iso_match_list"].apply(lambda x: not any(x))].copy()

adm1_iso_gdf6 = adm1_iso_gdf5.drop_duplicates(subset=['id', 'shapeGroup', 'Recipient ISO-3']).copy()

output_cols = ['id', 'shapeID', 'shapeGroup', 'shapeName', 'intersection_ratio', 'Recipient ISO-3', 'matching_iso', 'iso_match_list']
adm1_iso_gdf6[output_cols].to_csv(adm_data_dir / f"{output_tag}_adm1_iso_mismatch.csv", index=False)

adm1_iso_gdf7 = adm1_iso_gdf6[output_cols].copy()
adm1_iso_gdf7.drop(columns=["iso_match_list"], inplace=True)
adm1_iso_gdf7 = adm1_iso_gdf7.merge(raw_china_gdf[["id", "geometry"]], on="id", how="left")
adm1_iso_gdf7 = gpd.GeoDataFrame(adm1_iso_gdf7, geometry="geometry", crs="EPSG:4326")
adm1_iso_gdf7.to_file(adm_data_dir / f"{output_tag}_adm1_iso_mismatch.gpkg", driver='GPKG')
