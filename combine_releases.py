from pathlib import Path
import pandas as pd
import geopandas as gpd


base_path = Path('/home/userx/Desktop/tuff_osm')

lookup_2020usd_path = base_path / 'input_data/3.0test20182020/internalexport.xlsx'
lookup_2020usd_df = pd.read_excel(lookup_2020usd_path)
date_fields = ['Planned Implementation Start Date (MM/DD/YYYY)', 'Planned Completion Date (MM/DD/YYYY)', 'Actual Implementation Start Date (MM/DD/YYYY)', 'Actual Completion Date (MM/DD/YYYY)']
lookup_2020usd_df = lookup_2020usd_df[['AidData Tuff Project ID', 'Amount (Constant USD2020)'] + date_fields]
lookup_2020usd_df.columns = [c.replace(" ", ".") for c in lookup_2020usd_df.columns]

development_pre2018_path = base_path / 'output_data' / '2.0release' / 'results' / '2021_09_29_12_06' / 'development_combined_global.geojson'
all_20182020_path = base_path / 'output_data/3.0test20182020/results/2023_04_27_16_28/all_combined_global.geojson'

development_pre2018_gdf = gpd.read_file(development_pre2018_path, driver='GeoJSON')
development_pre2018_gdf.rename(columns={'AidData TUFF Project ID': 'AidData Tuff Project ID'}, inplace=True)
development_pre2018_gdf.drop(columns=['Amount (Constant USD2017)'], inplace=True)
development_pre2018_gdf.drop(columns=date_fields, inplace=True)
development_pre2018_gdf["precision"] = None
development_pre2018_gdf.columns = [c.replace(" ", ".") for c in development_pre2018_gdf.columns]

# development_pre2018_gdf.to_csv('/home/userx/Desktop/test_pre2018.csv', index=False)

all_20182020_gdf = gpd.read_file(all_20182020_path, driver='GeoJSON')
all_20182020_gdf.drop(columns=['Level of precision', 'Type of the OSM features', 'OSM links', 'version', 'viz_geojson_url', 'dl_geojson_url'], inplace=True)
all_20182020_gdf.drop(columns=[i.replace(" ", ".") for i in date_fields], inplace=True)
all_20182020_gdf.drop(columns=['Amount.(Constant.USD2020)'], inplace=True)

# all_20182020_gdf.to_csv('/home/userx/Desktop/test_20182020.csv', index=False)

assert sorted(development_pre2018_gdf.columns) == sorted(all_20182020_gdf.columns)

replacement_ids = [i for i in development_pre2018_gdf["AidData.Tuff.Project.ID"] if i in all_20182020_gdf["AidData.Tuff.Project.ID"]]
development_pre2018_gdf = development_pre2018_gdf.loc[~development_pre2018_gdf["AidData.Tuff.Project.ID"].isin(replacement_ids)]

combined_outputs_gdf = pd.concat([development_pre2018_gdf, all_20182020_gdf])
combined_outputs_gdf.drop(columns=['precision'], inplace=True)

combined_outputs_gdf = combined_outputs_gdf.merge(lookup_2020usd_df, on='AidData.Tuff.Project.ID', how='left')

for c in combined_outputs_gdf.columns:
    if c.endswith("Date.(MM/DD/YYYY)"):
        print("Converting to str:", c)
        combined_outputs_gdf[c] = combined_outputs_gdf[c].apply(lambda x: str(x) if not pd.isnull(x) else "")

combined_outputs_gdf['AidData.Tuff.Project.ID'] = combined_outputs_gdf['AidData.Tuff.Project.ID'].astype(int)
combined_outputs_gdf['Commitment.Year'] = combined_outputs_gdf['Commitment.Year'].apply(lambda x: str(int(x)) if x.is_integer() else "")

combined_path = base_path / 'output_data' / 'combined_2000_to_2020.geojson'
combined_outputs_gdf.to_file(combined_path, driver='GeoJSON')

# combined_outputs_gdf.to_csv('/home/userx/Desktop/combined_2000_to_2020.csv', index=False)
