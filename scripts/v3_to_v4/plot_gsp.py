"""Plot each gsp

Idea is to plot each new GSP with the old ones overlapping
"""

import json
import os

import geopandas as gpd
import plotly.graph_objects as go

import pvliveconsumer

file_v3 = "data/gsp_regions_20181031.geojson"
file_v4 = "data/gsp_regions_20220314.geojson"

dir = os.path.dirname(pvliveconsumer.__file__) + "/../scripts/v3_to_v4/"

v3_gdf = gpd.read_file(dir + file_v3)  # 329
v4_gdf = gpd.read_file(dir + file_v4)  # 333

v3_gdf = v3_gdf.to_crs(4326)
v4_gdf = v4_gdf.to_crs(4326)


def make_trace_from_geopandas(gdf, z):
    """Make a trace from a geopandas dataframe"""
    z_list = [z] * len(gdf)

    gdf = gdf.to_crs(4326)
    gdf_json = json.loads(gdf.to_json())

    trace = go.Choroplethmapbox(
        geojson=gdf_json,
        locations=gdf.index,
        z=z_list,
        zmax=2,
        zmin=1,
        marker={"opacity": 0.5},
    )

    return trace


# one gsp
for i in range(333):
    gsp_one = v4_gdf.iloc[i]
    gsp_one_gdf = v4_gdf.iloc[i : i + 1]

    print(i, gsp_one.GSPs)

    d = {"GSPs": gsp_one.GSPs, "geometry": gsp_one.geometry}
    gsp_one_repeat = gpd.GeoDataFrame([d] * 329)
    overlap = v3_gdf[v3_gdf.intersection(gsp_one_repeat).area > 1e-6]

    trace_1 = make_trace_from_geopandas(overlap, 1)
    trace_2 = make_trace_from_geopandas(gsp_one_gdf, 2)

    # make trace
    fig = go.Figure(data=[trace_1, trace_2])
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=4,
        mapbox_center={"lat": 55, "lon": 0},
    )
    # fig.show(renderer='browser')

    fig.write_html(f"{dir}plots/{gsp_one.GSPs}.html")
