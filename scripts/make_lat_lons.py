""" Make data/uk_gsp_locations.csv """

import geopandas as gpd
import pandas as pd
import requests, zipfile, io

#download zip file from the web
zip_file_url = 'https://api.neso.energy/dataset/2810092e-d4b2-472f-b955-d8bea01f9ec0/resource/d95e8c1b-9cd9-41dd-aacb-4b53b8c07c20/download/gsp_regions_20250109.zip'

# download zip file to local file
r = requests.get(zip_file_url)
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall("./temp/neso/")

import geopandas as gpd
shape = gpd.read_file("./temp/neso/Proj_4326/GSP_regions_4326_20250109.geojson")
shape['centroid'] = shape.geometry.centroid

shape['longitude'] = shape.centroid.x
shape['latitude'] = shape.centroid.y

data = shape[['GSPs','latitude', 'longitude']]
data.to_csv("pvliveconsumer/data/uk_gsp_locations.csv", index=True)

# load gsp id and gsps from 
from pvlive_api import PVLive
import pandas as pd
pvlive = PVLive(domain_url="api.pvlive.uk")
gsp_list = pvlive.gsp_list
gsp_list.set_index('gsp_name', inplace=True)

data = pd.read_csv("pvliveconsumer/data/uk_gsp_locations.csv")

# add gsp_id to the data
data['gsp_name'] = data['GSPs'].astype(str)
data.set_index('gsp_name', inplace=True)
data = data.join(gsp_list, on='gsp_name', how='left')

data = data[['gsp_id','latitude', 'longitude']]
data['gsp_id'] = data['gsp_id'].astype(int)

data.dropna(inplace=True)

# sort by gsp id
data.sort_values(by='gsp_id', inplace=True)

# save to csv
data.reset_index(drop=False, inplace=True)
data = data[['gsp_id','latitude', 'longitude','gsp_name']]

data.to_csv("pvliveconsumer/data/uk_gsp_locations.csv", index=False)

# manually then added
# 0,54.08963881116971,-2.6122022827776883,National