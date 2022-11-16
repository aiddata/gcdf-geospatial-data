"""
Generate donut using existing buffers

Requires existing buffers for specified outer/inner buffer sizes, and
expects you to be in the same directory as the buffered files
"""

import geopandas as gpd
from shapely.geometry import MultiPolygon


buffer_path_template = "development_combined_global_BUFFERm.geojson"

outer_buffer_size = 10000
inner_buffer_size = 5000

outer_buffer_path = buffer_path_template.replace("BUFFER", str(outer_buffer_size))
inner_buffer_path = buffer_path_template.replace("BUFFER", str(inner_buffer_size))

outer_buffer_gdf = gpd.read_file(outer_buffer_path)
inner_buffer_gdf = gpd.read_file(inner_buffer_path)


donut_gdf = outer_buffer_gdf.merge(inner_buffer_gdf[['id', 'geometry']], on='id')


def difference(outer, inner):
    try:
        donut = outer.difference(inner)
    except:
        try:
            donut = outer.buffer(0).difference(inner.buffer(0))
        except:
            return None
    try:
        donut = MultiPolygon(donut)
    except:
        donut = MultiPolygon([donut])

    return donut


donut_gdf['geometry'] = donut_gdf.apply(lambda x: difference(x['geometry_x'], x['geometry_y']), axis=1)

output_gdf = gpd.GeoDataFrame(donut_gdf.drop(columns=['geometry_x', 'geometry_y']))

output_path = f"{outer_buffer_size}m_{inner_buffer_size}m_donut.geojson"
output_gdf.to_file(output_path, driver="GeoJSON")
