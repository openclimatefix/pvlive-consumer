from datetime import UTC, datetime

from nowcasting_datamodel.models.gsp import GSPYield, Location, LocationSQL
from nowcasting_datamodel.read.read_gsp import get_latest_gsp_yield

from pvliveconsumer.gsps import filter_gsps_which_have_new_data, get_gsps


def test_get_gsps(db_session):
    gsps = get_gsps(session=db_session, n_gsps=10, regime="in-day")
    assert len(gsps) == 11  # (10 + national)


def test_filter_pv_systems_which_have_new_data_no_data(db_session):
    gsps = get_gsps(session=db_session, n_gsps=10, regime="in-day")
    gsps_keep = filter_gsps_which_have_new_data(gsps=gsps)

    assert len(gsps_keep) == 11  # (10 + national)


def test_filter_pv_systems_which_have_new_data(db_session):
    gsp_yield_0 = GSPYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=1).to_orm()
    gsp_yield_1 = GSPYield(datetime_utc=datetime(2022, 1, 1, 0, 30), solar_generation_kw=2).to_orm()
    gsp_yield_2 = GSPYield(datetime_utc=datetime(2022, 1, 1, 0, 34), solar_generation_kw=3).to_orm()

    gsps = [
        Location(gsp_id=1, label="GSP_1").to_orm(),
        Location(gsp_id=2, label="GSP_2").to_orm(),
        Location(gsp_id=3, label="GSP_3").to_orm(),
    ]

    gsp_yield_0.location = gsps[0]
    gsp_yield_1.location = gsps[1]
    gsp_yield_2.location = gsps[2]

    db_session.add_all([gsp_yield_0, gsp_yield_1, gsp_yield_2])
    db_session.add_all(gsps)

    gsps: list[LocationSQL] = db_session.query(LocationSQL).all()
    gsps = get_latest_gsp_yield(session=db_session, gsps=gsps, append_to_gsps=True)

    #
    #   | last data  | keep?
    # 1 | 35 mins    | True
    # 2 | 5 mins     | False
    # 3 | 1 mins     | False

    gsps_keep = filter_gsps_which_have_new_data(
        gsps=gsps, datetime_utc=datetime(2022, 1, 1, 0, 35, tzinfo=UTC),
    )

    assert len(gsps_keep) == 1
    assert gsps_keep[0].id == 1
