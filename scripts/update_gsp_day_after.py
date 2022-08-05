"""
Update database with gsp yield values. Before was pulling the PV live data to early and values werent updated

2. load installed capacity mw from pv live
3. Load gsp_yield from database
4. update database
"""


import json
from datetime import datetime, timezone

import boto3
import pandas as pd
import numpy as np
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models.gsp import Location
from nowcasting_datamodel.read.read import get_all_locations, get_location, national_gb_label
from nowcasting_datamodel.read.read_gsp import get_gsp_yield
from nowcasting_datamodel import N_GSP
from pvlive_api import PVLive

client = boto3.client("secretsmanager")
response = client.get_secret_value(
    SecretId="development/rds/forecast/",
)
secret = json.loads(response["SecretString"])
""" We have used a ssh tunnel to 'localhost' """
db_url = f'postgresql://{secret["username"]}:{secret["password"]}@localhost:5433/{secret["dbname"]}'

connection = DatabaseConnection(url=db_url, base=Base_Forecast, echo=True)
print('connection to pv')
pvl = PVLive()

with connection.get_session() as session:
    print('looping over gsps')
    i=0
    for gsp_id in range(15,N_GSP+1):
        i = i+1
        print(f'{gsp_id=}')

        # load installed capacity from pv live
        print("Getting information pv live")
        start = datetime(2022, 8, 1, tzinfo=timezone.utc)
        end = datetime(2022, 8, 3, tzinfo=timezone.utc)
        d = pvl.between(
            start=start,
            end=end,
            entity_type="gsp",
            dataframe=True,
            entity_id=gsp_id,
        )

        print('Getting gsp yields from databae')
        gsp_yields = get_gsp_yield(
            session=session,
            gsp_ids=[gsp_id],
            start_datetime_utc=start,
            end_datetime_utc=end,
            regime="day-after",
        )

        # update the values
        for gsp_yield in gsp_yields:
            datetime_utc = gsp_yield.datetime_utc
            datetime_utc = datetime_utc.replace(tzinfo=timezone.utc)

            solar_generation_mw = d[d["datetime_gmt"] == datetime_utc]["generation_mw"].iloc[0]
            old_value = gsp_yield.solar_generation_kw

            if old_value != solar_generation_mw:
                gsp_yield.solar_generation_kw = np.round(solar_generation_mw * 1000,4)
                print(
                    f"Updating gsp id {gsp_id} for {datetime_utc} with value {gsp_yield.solar_generation_kw} "
                    f"(it used to be {old_value})"
            )

        if i> 5:
            i=0
            # update database
            print("Updating database")
            session.commit()
            print("Updating database: done")

    # update database
    print("Updating database")
    session.commit()
    print("Updating database: done")
