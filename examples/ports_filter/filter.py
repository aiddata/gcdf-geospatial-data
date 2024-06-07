"""
Preparing a geodataframe of all projects related to ports in Central/South America from GeoGCDF v3
and retrieving associated project level data from GCDF v3


"""
from pathlib import Path
import requests
import zipfile
import shutil

import pandas as pd
import geopandas as gpd


base_path = Path("/home/userx/Desktop/tuff_osm/examples/ports_filter")


# =================================================================================================
# prep geocoded data

gcdf_path = base_path.parent.parent / "latest/all_combined_global.gpkg"

gcdf_gdf = gpd.read_file(gcdf_path, driver="GPKG")

def port_related(raw_title):
    title = raw_title.lower()

    exact_keywords = ["Port", " port ", " port."]
    related_keywords = ["port", "harbour", "terminal", "dock", "wharf", "berth", "quay", "jetty"]
    unrelated_keywords = ["airport", "sport"]
    vague_keywords = ["import", "export", "report", "support", "transport"]

    if any([i in title for i in unrelated_keywords]):
        return False

    if not any([i in title for i in related_keywords]):
        return False

    if any([i in title for i in vague_keywords]):
        if not any([i in raw_title for i in exact_keywords]):
            return False

    return True


gcdf_gdf["port_related"] = gcdf_gdf.apply(lambda x: port_related(x.Title), axis=1)
port_gdf = gcdf_gdf[gcdf_gdf["port_related"]].copy()
# port_gdf.Title.to_list()

port_gdf = port_gdf.loc[port_gdf.geometry.centroid.x < -30].copy()

invalid_sector_list = ["HEALTH", "EDUCATION", "GOVERNMENT AND CIVIL SOCIETY", "AGRICULTURE, FORESTRY, FISHING", "WATER SUPPLY AND SANITATION"]
port_gdf = port_gdf.loc[~port_gdf["Sector.Name"].isin(invalid_sector_list)].copy()

port_gdf["precise"] = port_gdf["osm_precision_list"].apply(lambda x: "precise" in x)
port_gdf["approximate"] = port_gdf["osm_precision_list"].apply(lambda x: "approximate" in x)
port_gdf["adm"] = port_gdf["osm_precision_list"].apply(lambda x: "adm" in x)

port_gdf.to_file(base_path / "related.gpkg", driver="GPKG")

# manually select relevant projects
relevant_id_list = [89653, 85355, 69333, 73745, 54912, 43110, 41465]
manual_gdf = port_gdf.loc[port_gdf.id.isin(relevant_id_list)].copy()
manual_gdf.to_file(base_path / "specific.gpkg", driver="GPKG")


# =================================================================================================
# prep project level data

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
adm1_df = pd.read_csv(adm1_path)
adm2_df = pd.read_csv(adm2_path)

port_proj_df = proj_df[proj_df["AidData Record ID"].isin(port_gdf["id"])].copy()
port_adm1_df = adm1_df[adm1_df["id"].isin(port_gdf["id"])].copy()
port_adm2_df = adm2_df[adm2_df["id"].isin(port_gdf["id"])].copy()

(base_path / "project_level_data").mkdir(exist_ok=True, parents=True)

port_proj_df.to_csv(base_path / "project_level_data" / "projects.csv", index=False)
port_adm1_df.to_csv(base_path / "project_level_data" / "adm1.csv", index=False)
port_adm2_df.to_csv(base_path / "project_level_data" / "adm2.csv", index=False)

# copy project docs
(proj_base / "Field Definitions_GCDF 3.0.pdf").rename(base_path / "project_level_data" / "Field Definitions_GCDF 3.0.pdf")
(proj_base / "TUFF Methodology 3.0.pdf").rename(base_path / "project_level_data" / "TUFF Methodology 3.0.pdf")

# delete raw zip and extracted files
zip_path.unlink()
shutil.rmtree(proj_base)
