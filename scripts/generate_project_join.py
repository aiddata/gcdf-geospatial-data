import ast

import geopandas as gpd

# ==========================================================
# create file of data to be joined back into project level GCDF data


pj_in_path = '/home/userx/Desktop/tuff_osm/output_data/gcdf_v3/results/2023_10_12_13_22/all_combined_global.gpkg'
pj_out_path = '/home/userx/Desktop/tuff_osm/output_data/gcdf_v3/results/2023_10_12_13_22/project_join.csv'

pj_in = gpd.read_file(pj_in_path)

pj_df = pj_in[["id", "osm_precision_list"]].copy()


pj_df.osm_precision_list = pj_df.osm_precision_list.apply(lambda x: list(set(ast.literal_eval(x))))

# all data produced here will have a geospatial feature
pj_df["has_geospatial_feature"] = "Yes"


"""
# all data will be at adm1 level or finer
#   check for adm0-adm3 to be safe
bad_adm_df = pj_df.loc[pj_df.osm_precision_list.apply(lambda x: any([i in x for i in ('adm0', 'adm1', 'adm2')]))]
bad_adm_gdf = bad_adm_df.merge(combined_gdf, on='id', how='left')
bad_adm_gdf = gpd.GeoDataFrame(bad_adm_gdf)
bad_adm_gdf["precision_str"] = bad_adm_gdf.osm_precision_list_x.apply(lambda x: ",".join(x))

bad_adm_gdf[[i for i in bad_adm_gdf.columns if 'osm_precision' not in i]].to_file(output_dir / 'bad_adm.gpkg', driver='GPKG')
bad_adm_gdf[[i for i in bad_adm_gdf.columns if i != 'geometry']].to_csv(output_dir / 'bad_adm.csv', index=False)
"""

pj_df["adm1_compatible"] = pj_df.osm_precision_list.apply(lambda x: all([i not in x for i in ('adm0', 'adm1', 'adm2')])).astype(int)

# drop adm0-adm5
pj_df["adm2_compatible"] = pj_df.osm_precision_list.apply(lambda x: all([i not in x for i in ('adm0', 'adm1', 'adm2', 'adm3', 'adm4', 'adm5')])).astype(int)

pj_df["adm1_compatible"].replace({0: "No", 1: "Yes"}, inplace=True)
pj_df["adm2_compatible"].replace({0: "No", 1: "Yes"}, inplace=True)


# best precision value from precision list
#   use human readable values
def get_best_precision(x):
    if "precise" in x:
        return "precise"
    elif "approximate" in x:
        return "approximate"
    else:
        return "adm"

def get_worst_precision(x):
    if any(["adm" in i for i in x]):
        return "adm"
    elif "approximate" in x:
        return "approximate"
    else:
        return "precise"

pj_df["best_precision_field"] = pj_df.osm_precision_list.apply(lambda x: get_best_precision(x))
pj_df["worst_precision_field"] = pj_df.osm_precision_list.apply(lambda x: get_worst_precision(x))
pj_df["best_same_as_worst_precision"] = pj_df.best_precision_field == pj_df.worst_precision_field

pj_df["best_precision_field"].replace({"precise": "Precise", "adm": "Admin Level", "approximate": "Within 5km"}, inplace=True)
pj_df["worst_precision_field"].replace({"precise": "Precise", "adm": "Admin Level", "approximate": "Within 5km"}, inplace=True)

pj_df["has_geospatial_feature"].value_counts()
pj_df["adm1_compatible"].value_counts()
pj_df["adm2_compatible"].value_counts()

pj_df["best_precision_field"].value_counts()
pj_df["worst_precision_field"].value_counts()
pj_df["best_same_as_worst_precision"].value_counts()

"""
has_geospatial_feature
Yes    9459

adm1_compatible
Yes    9459

adm2_compatible
Yes    8075
No     1384

best_precision_field
Precise        6510
Admin Level    2343
Within 5km      606

worst_precision_field
Precise        6299
Admin Level    2539
Within 5km      621

best_same_as_worst_precision
True     9199
False     260
"""

pj_final_df = pj_df.drop(columns=["osm_precision_list"]).copy()
pj_final_df.to_csv(pj_out_path, index=False)
