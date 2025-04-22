from nowcasting_datamodel.connection import Base_Forecast, DatabaseConnection
from nowcasting_datamodel.read.read import get_location
from pvlive_api import PVLive

pvlive = PVLive(domain_url="api.pvlive.uk")
gsp_ids = pvlive.gsp_ids
gsp_list = pvlive.gsp_list[::-1]

# pvlive.latest(entity_type="gsp", entity_id=320)

db_url = "TODO"

connection = DatabaseConnection(url=db_url, base=Base_Forecast, echo=True)

with connection.get_session() as session:
    for i, gsp_detail in gsp_list.iterrows():
        if gsp_id == 0:
            pass
        else:
            gsp_id = int(gsp_detail.gsp_id)
            print(gsp_id)

            location = get_location(session=session, gsp_id=gsp_id)
            location.gsp_name = gsp_detail.gsp_name

            if location.region_name is None:
                location.region_name = gsp_detail.gsp_name

    session.commit()

    # load installed capacity from pv live
