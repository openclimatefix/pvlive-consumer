from datetime import UTC, datetime

from nowcasting_datamodel.models.gsp import GSPYield, GSPYieldSQL, Location

from pvliveconsumer.backup import get_number_gsp_yields, make_gsp_yields_from_national


def add_national_gsp_yields(db_session):
    gsp_yield_0 = GSPYield(datetime_utc=datetime(2022, 1, 1), solar_generation_kw=1).to_orm()

    gsps = [
        Location(gsp_id=0, label="national", installed_capacity_mw=10).to_orm(),
    ]

    gsp_yield_0.location = gsps[0]

    db_session.add_all([gsp_yield_0])
    db_session.add_all(gsps)


def add_gsp_yields(db_session):
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

    return gsps


def test_get_number_gsp_yields_empty(db_session):
    start = datetime_utc = datetime(2022, 1, 1, 0, 35, tzinfo=UTC)
    end = datetime_utc = datetime(2022, 1, 2, 0, 35, tzinfo=UTC)
    regime = "in-day"

    n_gsp_yields_sql = get_number_gsp_yields(
        session=db_session, start_datetime_utc=start, end_datetime_utc=end, regime=regime,
    )
    assert n_gsp_yields_sql == 0


def test_get_number_gsp_yields(db_session):
    add_gsp_yields(db_session)
    add_national_gsp_yields(db_session)

    start = datetime_utc = datetime(2022, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime_utc = datetime(2022, 1, 2, 0, 0, tzinfo=UTC)
    regime = "in-day"

    n_gsp_yields_sql = get_number_gsp_yields(
        session=db_session, start_datetime_utc=start, end_datetime_utc=end, regime=regime,
    )
    assert n_gsp_yields_sql == 3


def test_make_gsp_yields_from_national_not_needed(db_session):
    locations = add_gsp_yields(db_session)
    add_national_gsp_yields(db_session)

    start = datetime_utc = datetime(2022, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime_utc = datetime(2022, 1, 2, 0, 0, tzinfo=UTC)
    regime = "in-day"

    gsp_yields = make_gsp_yields_from_national(
        session=db_session, start=start, end=end, regime=regime, locations=locations,
    )
    assert len(gsp_yields) == 0


def test_make_gsp_yields_from_national(db_session):
    locations = gsps = [
        Location(gsp_id=1, label="GSP_1", installed_capacity_mw=2).to_orm(),
        Location(gsp_id=2, label="GSP_2", installed_capacity_mw=3).to_orm(),
        Location(gsp_id=3, label="GSP_3", installed_capacity_mw=4).to_orm(),
    ]
    add_national_gsp_yields(db_session)

    start = datetime_utc = datetime(2022, 1, 1, 0, 0, tzinfo=UTC)
    end = datetime_utc = datetime(2022, 1, 2, 0, 0, tzinfo=UTC)
    regime = "in-day"

    gsp_yields = make_gsp_yields_from_national(
        session=db_session, start=start, end=end, regime=regime, locations=locations,
    )
    assert len(gsp_yields) == 3
    assert isinstance(gsp_yields[0], GSPYieldSQL)
    assert gsp_yields[0].solar_generation_kw == 2 / 10
    assert gsp_yields[1].solar_generation_kw == 3 / 10
    assert gsp_yields[2].solar_generation_kw == 4 / 10
