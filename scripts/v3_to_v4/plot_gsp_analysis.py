"""
Analysis the amount that each GSP overlaps with others

Plots
1. See how many installed capacity overlaps by a GSP over lap threshold
2. Plot GSP overlaps values (from v3-to-v4) on a UK map
"""

import json
import os
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
from nowcasting_dataset.data_sources.gsp.eso import get_gsp_metadata_from_eso

# get installed capacity from pv live (this takes ~30 seconds)
from nowcasting_dataset.data_sources.gsp.pvlive import get_installed_capacity

import gspconsumer

file_v3 = "data/gsp_regions_20181031.geojson"
file_v4 = "data/gsp_regions_20220314.geojson"

dir = os.path.dirname(gspconsumer.__file__) + "/../scripts/v3_to_v4/"

# load data
v3_gdf = gpd.read_file(dir + file_v3)  # 329
v4_gdf = gpd.read_file(dir + file_v4)  # 333

# change to lat lon
v3_gdf = v3_gdf.to_crs(4326)
v4_gdf = v4_gdf.to_crs(4326)

print("getting installed capacity ...")
installed_capacity = get_installed_capacity(start=datetime(2022, 7, 1, tzinfo=timezone.utc))
print("getting installed capacity: done")

# getting metadata from eso and joining with installed capacity (v3)
metadata = get_gsp_metadata_from_eso()
metadata = metadata.drop_duplicates(subset="region_id", keep="first")
metadata.set_index("gsp_id", inplace=True, drop=False)
metadata = metadata.join(installed_capacity)
metadata.set_index("region_id", inplace=True)
metadata = metadata[["gsp_name", "installedcapacity_mwp", "gsp_id"]]


# loop over all  gsp
save = []
v4_gdf["overlaping_per"] = 0.0
for i in range(333):
    gsp_one = v4_gdf.iloc[i]
    gsp_one_gdf = v4_gdf.iloc[i : i + 1]

    print(i, gsp_one.GSPs)

    # find overlapping regions with v3
    d = {"GSPs": gsp_one.GSPs, "geometry": gsp_one.geometry}
    gsp_one_repeat = gpd.GeoDataFrame([d] * 329)
    overlap = v3_gdf[v3_gdf.intersection(gsp_one_repeat).area > 1e-6]
    overlap.reset_index(inplace=True, drop=True)

    # lets see how much overlap there is
    gsp_one_repeat = gpd.GeoDataFrame([d] * len(overlap))
    area_v4 = gsp_one_gdf.area
    overlap.loc[:, "overlap_area"] = overlap.intersection(gsp_one_repeat).area
    overlap.loc[:, "overlap_area_per"] = overlap["overlap_area"] / area_v4.iloc[0]

    # join with metadata
    overlap.index = overlap["RegionID"]
    overlap = overlap.join(metadata)

    # pick largest overlapping area
    idx = overlap["overlap_area_per"].idxmax()
    max = overlap.loc[overlap["overlap_area_per"].idxmax()]
    save.append([max.RegionID, max.overlap_area_per, max.gsp_name, max.installedcapacity_mwp])

    v4_gdf.loc[i, "overlaping_per"] = max.overlap_area_per


data_df = pd.DataFrame(
    data=save, columns=["region_id", "overlap_per", "gsp_name", "installedcapacity_mwp"]
)
data_df.sort_values("overlap_per", ascending=False, inplace=True)
data_df["installedcapacity_mwp_cumsum"] = (
    data_df["installedcapacity_mwp"].cumsum() / data_df["installedcapacity_mwp"].sum()
)

trace_1 = go.Scatter(y=data_df["overlap_per"], name="overlap")
trace_2 = go.Scatter(y=data_df["installedcapacity_mwp_cumsum"], name="installed_capacity_cumsum")

# make trace
fig = go.Figure(data=[trace_1, trace_2])
fig.update_layout(
    title="Compare v3 and v4, overlapping regiong",
    xaxis_title="GSP number, order by overlaping region",
    yaxis_title="[%]",
)
# fig.show(renderer='browser')

fig.write_html(f"{dir}plots/scatter_overlapping.html")

# make map plot
v4_gdf = v4_gdf.to_crs(4326)
v4_gdf_json = json.loads(v4_gdf.to_json())

trace = go.Choroplethmapbox(
    geojson=v4_gdf_json,
    locations=v4_gdf.index,
    z=v4_gdf["overlaping_per"],
    zmax=1,
    zmin=0,
    # marker={"opacity": 0.5},
)

fig = go.Figure(data=trace)
fig.update_layout(mapbox_style="carto-positron", mapbox_zoom=4, mapbox_center={"lat": 55, "lon": 0})
# fig.show(renderer='browser')

fig.write_html(f"{dir}plots/overlapping_map.html")
