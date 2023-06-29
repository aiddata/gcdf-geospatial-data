from pathlib import Path
import pandas as pd
import geopandas as gpd


base_path = Path('/home/userx/Desktop/tuff_osm')

old_path = base_path / "output_data" / "combined_2000_to_2020_excludenew.geojson"
new_path = base_path / "output_data/3.0test2021/results/2023_06_28_14_15" / "all_combined_global.geojson"

old_gdf = gpd.read_file(old_path, driver='GeoJSON')
new_gdf = gpd.read_file(new_path, driver='GeoJSON')


new_gdf.columns = [c.replace(" ", ".") for c in new_gdf.columns]
new_gdf.drop(columns=['precision', 'Level.of.prevision', 'Type.of.the.OSM.features', 'OSM.links', 'version', 'viz_geojson_url', 'dl_geojson_url'], inplace=True)

new_gdf.rename(columns={'Actual.Implementation.Start.Date.Estimated': 'Actual.Completion.Date.(MM/DD/YYYY)'}, inplace=True)

# old_gdf.columns
# new_gdf.columns

# [i for i in old_gdf.columns if i not in new_gdf.columns]
# [i for i in new_gdf.columns if i not in old_gdf.columns]


combined_gdf = pd.concat([old_gdf, new_gdf], ignore_index=True)
# combined_gdf.columns

combined_path = base_path / 'output_data' / 'combined_2000_to_2021.geojson'
combined_gdf.to_file(combined_path, driver='GeoJSON')
