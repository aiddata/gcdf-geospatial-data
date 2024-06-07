"""
Preparing a geodataframe of all projects in Malaysia from GeoGCDF v3
and retrieving/filtering project level data for Malaysia from GCDF v3


"""
from pathlib import Path
import requests
import zipfile
import shutil

import pandas as pd
import geopandas as gpd


base_path = Path("/home/userx/Desktop/tuff_osm/examples/malaysia_filter")


# =================================================================================================
# prep geocoded data for malaysia

gcdf_path = base_path.parent.parent / "latest/all_combined_global.gpkg"

gcdf_gdf = gpd.read_file(gcdf_path, driver="GPKG")

mys_gdf = gcdf_gdf[gcdf_gdf["Recipient"] == "Malaysia"].copy()
mys_gdf["precise"] = mys_gdf["osm_precision_list"].apply(lambda x: "precise" in x)
mys_gdf["approximate"] = mys_gdf["osm_precision_list"].apply(lambda x: "approximate" in x)
mys_gdf["adm"] = mys_gdf["osm_precision_list"].apply(lambda x: "adm" in x)

mys_gdf.to_file(base_path / "china_malaysia.gpkg", driver="GPKG")


mys_precise_gdf = mys_gdf.loc[~mys_gdf.adm].copy()
mys_precise_gdf.to_file(base_path / "malaysia_precise.gpkg", driver="GPKG")


mys_high_value_gdf = mys_gdf[mys_gdf["Amount.(Constant.USD.2021)"] > 1e6].copy()
mys_high_value_gdf.to_file(base_path / "malaysia_high_value.gpkg", driver="GPKG")


# =================================================================================================
# prep project level data for malaysia

# download project level data
zip_url = "https://docs.aiddata.org/ad4/datasets/AidDatas_Global_Chinese_Development_Finance_Dataset_Version_3_0.zip"
zip_path = base_path / Path(zip_url).name

if not zip_path.exists():
    r = requests.get(zip_url)
    with open(zip_path, "wb") as f:
        f.write(r.content)

# extract zip
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    extract_list = [i for i in zip_ref.namelist() if not i.startswith("__MACOSX")]
    zip_ref.extractall(base_path, members=extract_list)

# read project level data
proj_base = base_path / "AidDatas_Global_Chinese_Development_Finance_Dataset_Version_3_0"
proj_path = proj_base / "AidDatasGlobalChineseDevelopmentFinanceDataset_v3.0.xlsx"

adm1_path = proj_base / "ADM Location Data" / "GCDF_3.0_ADM1_Locations.csv"
adm2_path = proj_base / "ADM Location Data" / "GCDF_3.0_ADM2_Locations.csv"

proj_df = pd.read_excel(proj_path, sheet_name="GCDF_3.0")

mys_proj_df = proj_df[proj_df["Recipient"] == "Malaysia"].copy()

adm1_df = pd.read_csv(adm1_path)
adm2_df = pd.read_csv(adm2_path)

mys_adm1_df = adm1_df[adm1_df["shapeGroup"] == "MYS"].copy()
mys_adm2_df = adm2_df[adm2_df["shapeGroup"] == "MYS"].copy()

(base_path / "project_level_data").mkdir(exist_ok=True, parents=True)

mys_proj_df.to_csv(base_path / "project_level_data" / "malaysia_projects.csv", index=False)
mys_adm1_df.to_csv(base_path / "project_level_data" / "malaysia_adm1.csv", index=False)
mys_adm2_df.to_csv(base_path / "project_level_data" / "malaysia_adm2.csv", index=False)

# copy project docs
(proj_base / "Field Definitions_GCDF 3.0.pdf").rename(base_path / "project_level_data" / "Field Definitions_GCDF 3.0.pdf")
(proj_base / "TUFF Methodology 3.0.pdf").rename(base_path / "project_level_data" / "TUFF Methodology 3.0.pdf")

# delete raw zip and extracted files
zip_path.unlink()
shutil.rmtree(proj_base)
