

import requests
from pathlib import Path
import ast

import pandas as pd
import geopandas as gpd


dry_run = False
intersection_threshold = 0.0001

output_tag = "gcdf_v3"

dataset_path = "/home/userx/Desktop/tuff_osm/output_data/gcdf_v3/results/2023_10_12_13_22/all_combined_global.gpkg"

project_data_path = "/home/userx/Desktop/tuff_osm/input_data/gcdf_v3/cdf2021.csv"
value_field = "Amount.(Constant.USD2021)"
# id_field = "AidData Tuff Project ID"

# geo_gdf = gpd.read_file(dataset_path)
# project_df = pd.read_csv(project_data_path)
# project_df = project_df[[id_field, value_field]]

# china_gdf = geo_gdf.merge(project_df, left_on="id", right_on=id_field, how="left")

raw_china_gdf = gpd.read_file(dataset_path)
raw_china_gdf.osm_precision_list = raw_china_gdf.osm_precision_list.apply(lambda x: list(set(ast.literal_eval(x))))

# =====================================
# init and download adm data if needed

adm_data_dir = Path("/home/userx/Desktop/tuff_osm/gb_v6")
adm_data_dir.mkdir(parents=True, exist_ok=True)

# gb v6 release
adm1_cgaz_path = f"https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/CGAZ/geoBoundariesCGAZ_ADM1.gpkg"
adm2_cgaz_path = f"https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/CGAZ/geoBoundariesCGAZ_ADM2.gpkg"

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

# =====================================
# load adm data

adm1_gdf = gpd.read_file(adm1_dst_path, driver='GPKG')
adm1_gdf.geometry = adm1_gdf.geometry.buffer(0)
adm1_gdf = adm1_gdf.loc[adm1_gdf.shapeID.notnull()].copy()

adm2_gdf = gpd.read_file(adm2_dst_path, driver='GPKG')
adm2_gdf.geometry = adm2_gdf.geometry.buffer(0)
adm2_gdf = adm2_gdf.loc[adm2_gdf.shapeID.notnull()].copy()


# =====================================

# for missing projects, buffer incrementally and run again until they intersect,
# making a note in field of buffer size needed to intersect and
# returning buffered geometry to use for intersections later
def adm_lookup(project_gdf, adm_gdf, buffer_size=0, max_buffer_size=0.2):
    # print(f"Count: {len(project_gdf)}")
    # print(f"Buffer size: {buffer_size}")
    project_gdf["buffer_size"] = buffer_size
    project_gdf["geometry"] = project_gdf["geometry"].buffer(buffer_size)
    project_gdf["adm_id"] = project_gdf["geometry"].apply(lambda x: adm_gdf[adm_gdf.intersects(x)].shapeID.to_list())
    valid_gdf = project_gdf.loc[project_gdf.adm_id.apply(lambda x: len(x) > 0)].copy()
    missing_adm_ids = project_gdf.loc[project_gdf.adm_id.apply(lambda x: len(x) == 0), 'id'].to_list()
    if len(missing_adm_ids) == 0:
        return valid_gdf
    else:
        missing_gdf = project_gdf.loc[project_gdf.id.isin(missing_adm_ids)][["id", "geometry"]].copy()
        if buffer_size >= max_buffer_size:
            final_gdf = pd.concat([valid_gdf, missing_gdf])
        else:
            missing_gdf = project_gdf.loc[project_gdf.id.isin(missing_adm_ids)][["id", "geometry"]].copy()
            solved_gdf = adm_lookup(missing_gdf, adm_gdf, buffer_size=buffer_size+0.025)
            final_gdf = pd.concat([valid_gdf, solved_gdf])
        return final_gdf



base_china_adm1_gdf = raw_china_gdf[["id", value_field, "geometry"]].copy()
matched_china_adm1_gdf = adm_lookup(base_china_adm1_gdf.copy(), adm1_gdf)
matched_china_adm1_gdf.loc[matched_china_adm1_gdf.buffer_size != 0][["id", "geometry", "buffer_size"]].to_file(adm_data_dir / f"{output_tag}_adm1_buffered.gpkg", driver="GPKG")


base_china_adm2_gdf = raw_china_gdf.loc[raw_china_gdf.osm_precision_list.apply(lambda x: all([i not in x for i in ('adm0', 'adm1', 'adm2', 'adm3', 'adm4', 'adm5')]))]

base_china_adm2_gdf = base_china_adm2_gdf[["id", value_field, "geometry"]].copy()
matched_china_adm2_gdf = adm_lookup(base_china_adm2_gdf.copy(), adm2_gdf)
matched_china_adm2_gdf.loc[matched_china_adm2_gdf.buffer_size != 0][["id", "geometry", "buffer_size"]].to_file(adm_data_dir / f"{output_tag}_adm2_buffered.gpkg", driver="GPKG")


# =====================================
# summarize buffered/missing projects for adm1/adm2


buffer_summary = f"""
Buffered/missing projects overview

Project Count: {len(raw_china_gdf)}

ADM1:
\t No buffer needed: {len(matched_china_adm1_gdf.loc[matched_china_adm1_gdf.buffer_size == 0])}
\t <=5km buffer: {len(matched_china_adm1_gdf.loc[(matched_china_adm1_gdf.buffer_size <= 0.05) & (matched_china_adm1_gdf.buffer_size > 0)])}
\t >5km buffer: {len(matched_china_adm1_gdf.loc[(matched_china_adm1_gdf.buffer_size > 0.05)])}
\t No match with max buffer: {len(matched_china_adm1_gdf.loc[matched_china_adm1_gdf.buffer_size.isna()])}



ADM2:
\t No buffer needed: {len(matched_china_adm2_gdf.loc[matched_china_adm2_gdf.buffer_size == 0])}
\t <=5km buffer: {len(matched_china_adm2_gdf.loc[(matched_china_adm2_gdf.buffer_size <= 0.05) & (matched_china_adm1_gdf.buffer_size > 0)])}
\t >5km buffer: {len(matched_china_adm2_gdf.loc[(matched_china_adm2_gdf.buffer_size > 0.05)])}
\t No match with max buffer: {len(matched_china_adm2_gdf.loc[matched_china_adm2_gdf.buffer_size.isna()])}

"""

print(buffer_summary)

with open(adm_data_dir / f"{output_tag}_buffer_summary.txt", "w") as f:
    f.write(buffer_summary)




# =====================================
# run adm1

china_adm1_gdf = matched_china_adm1_gdf.explode("adm_id")
china_adm1_gdf.rename(columns={"adm_id": "shapeID"}, inplace=True)
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
china_adm1_gdf["intersection_ratio_commitment_value"] = china_adm1_gdf["intersection_ratio"] * china_adm1_gdf[value_field]
china_adm1_gdf["even_split_ratio_commitment_value"] = china_adm1_gdf["even_split_ratio"] * china_adm1_gdf[value_field]
china_adm1_gdf.drop(columns=[value_field], inplace=True)

china_adm1_gdf = china_adm1_gdf[["id", "shapeID", "shapeGroup", "shapeName", "intersection_ratio", "even_split_ratio", "intersection_ratio_commitment_value", "even_split_ratio_commitment_value", "centroid_longitude", "centroid_latitude", "geometry"]]

china_adm1_gdf["intersection_ratio"] = china_adm1_gdf["intersection_ratio"].astype(float).round(2)
china_adm1_gdf["even_split_ratio"] = china_adm1_gdf["intersection_ratio"].astype(float).round(2)
china_adm1_gdf["intersection_ratio_commitment_value"] = china_adm1_gdf["intersection_ratio_commitment_value"].astype(float).round(2)
china_adm1_gdf["intersection_ratio_commitment_value"] = china_adm1_gdf["intersection_ratio_commitment_value"].astype(float).round(2)

if not dry_run:
    china_adm1_gdf.to_file(adm_data_dir / f"{output_tag}_adm1.gpkg", driver="GPKG")
    china_adm1_gdf[[i for i in china_adm1_gdf.columns if i != "geometry"]].to_csv(adm_data_dir / f"{output_tag}_adm1.csv", index=False)

# =====================================
# run adm2

china_adm2_gdf = matched_china_adm2_gdf.explode("adm_id")
china_adm2_gdf.rename(columns={"adm_id": "shapeID"}, inplace=True)
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
china_adm2_gdf["intersection_ratio_commitment_value"] = china_adm2_gdf["intersection_ratio"] * china_adm2_gdf[value_field]
china_adm2_gdf["even_split_ratio_commitment_value"] = china_adm2_gdf["even_split_ratio"] * china_adm2_gdf[value_field]
china_adm2_gdf.drop(columns=[value_field], inplace=True)

china_adm2_gdf["intersection_ratio"] = china_adm2_gdf["intersection_ratio"].astype(float).round(2)
china_adm2_gdf["even_split_ratio"] = china_adm2_gdf["intersection_ratio"].astype(float).round(2)
china_adm2_gdf["intersection_ratio_commitment_value"] = china_adm2_gdf["intersection_ratio_commitment_value"].astype(float).round(2)
china_adm2_gdf["intersection_ratio_commitment_value"] = china_adm2_gdf["intersection_ratio_commitment_value"].astype(float).round(2)

china_adm2_gdf = china_adm2_gdf[["id", "shapeID", "shapeGroup", "shapeName", "intersection_ratio", "even_split_ratio", "intersection_ratio_commitment_value", "even_split_ratio_commitment_value","centroid_longitude", "centroid_latitude", "geometry"]]

if not dry_run:
    china_adm2_gdf.to_file(adm_data_dir / f"{output_tag}_adm2.gpkg", driver="GPKG")
    china_adm2_gdf[[i for i in china_adm2_gdf.columns if i != "geometry"]].to_csv(adm_data_dir / f"{output_tag}_adm2.csv", index=False)


# =====================================
# genearte crude hierarchy of adm1/adm2 units

project_filtered_adm1_gdf = adm1_gdf.loc[adm1_gdf.shapeID.isin(list(china_adm1_gdf.shapeID.unique()))].copy()
project_filtered_adm1_gdf = project_filtered_adm1_gdf[["shapeID", "shapeName", "shapeGroup", "geometry"]].copy()

project_filtered_adm2_gdf = adm2_gdf.loc[adm2_gdf.shapeID.isin(list(china_adm2_gdf.shapeID.unique()))].copy()
project_filtered_adm2_gdf = project_filtered_adm2_gdf[["shapeID", "shapeName", "shapeGroup", "geometry"]].copy()

project_filtered_adm2_centroid_gdf = project_filtered_adm2_gdf.copy()
project_filtered_adm2_centroid_gdf["geometry"] = project_filtered_adm2_gdf["geometry"].centroid


x = gpd.sjoin(project_filtered_adm1_gdf, project_filtered_adm2_gdf, how="inner", predicate="intersects", lsuffix="adm1", rsuffix="adm2")

y = x.merge(project_filtered_adm2_gdf[["shapeID", "geometry"]], left_on="shapeID_adm2", right_on="shapeID", suffixes=("", "_adm2"), how="inner")
y["intersection_ratio"] = y["geometry"].intersection(y["geometry_adm2"]).area / y["geometry_adm2"].area
y["country_agree"] = y["shapeGroup_adm1"] == y["shapeGroup_adm2"]

z = y[[i for i in y.columns if i not in ["geometry", "geometry_adm2", "shapeID", "index_adm2"]]].copy()
z = z.loc[z["country_agree"] == True].copy()
z["shapeGroup"] = z["shapeGroup_adm1"]
z.drop(columns=["shapeGroup_adm1", "shapeGroup_adm2", "country_agree"], inplace=True)

z1 = z.sort_values('intersection_ratio', ascending=False).drop_duplicates(['shapeID_adm2'])

z1.to_csv(adm_data_dir / f"{output_tag}_adm1_adm2_hierarchy.csv", index=False)
