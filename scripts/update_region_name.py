"""
Update database with gsp region

1. load csv file
2. update database
"""


import json

import boto3
import pandas as pd
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.read.read import get_all_locations, get_location
from pvlive_api import PVLive

# laod database secret from AWS secrets
client = boto3.client("secretsmanager")
response = client.get_secret_value(
    SecretId="development/rds/forecast/",
)
secret = json.loads(response["SecretString"])
""" We have used a ssh tunnel to 'localhost' """
db_url = f'postgresql://{secret["username"]}:{secret["password"]}@localhost:5433/{secret["dbname"]}'

# make database connection
connection = DatabaseConnection(url=db_url, base=Base_Forecast, echo=True)

# load new region names
data_df = pd.read_Csv("xxx.csv")
# this has columns 'gsp_id' and 'region_name'

with connection.get_session() as session:
    locations = get_all_locations(session=session)

    for row in data_df:

        location = get_location(session=session, gsp_id=row.gsp_id)
        location.region_name = row.region_name

    session.commit()
