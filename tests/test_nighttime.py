from pvliveconsumer.nightime import make_night_time_zeros
from datetime import datetime
from nowcasting_datamodel.models.gsp import LocationSQL, GSPYieldSQL
import pandas as pd


from nowcasting_datamodel.read.read import get_location


def test_make_night_time_zeros_wrong_regime():
    start = datetime(2021, 1, 1)
    end = datetime(2021, 1, 2)
    gsp = LocationSQL(gsp_id=1)
    gsp_yield_df = pd.DataFrame()
    regime = "wrong"

    result = make_night_time_zeros(start, end, gsp, gsp_yield_df, regime)

    assert result.equals(gsp_yield_df)


def test_make_night_time_zeros(db_session):
    start = datetime(2021, 1, 1)
    end = datetime(2021, 1, 2)
    gsp = get_location(session=db_session, gsp_id=1)
    gsp.last_gsp_yield = None
    gsp_yield_df = pd.DataFrame()
    regime = "in-day"

    result = make_night_time_zeros(start, end, gsp, gsp_yield_df, regime)

    # there should be around 18 hours of darkness which is 28 half hour intervals
    # sun set is ~16:00 and sun rise is ~8:00 for
    assert len(result) == 36
    assert result["generation_mw"].sum() == 0


def test_make_night_time_zeros_with_last_gsp_yield(db_session):
    start = datetime(2021, 1, 1)
    end = datetime(2021, 1, 2)
    gsp = get_location(session=db_session, gsp_id=1)
    gsp.last_gsp_yield = GSPYieldSQL(
        solar_generation_kw=1000, capacity_mwp=1.1, pvlive_updated_utc=start
    )
    gsp_yield_df = pd.DataFrame()
    regime = "in-day"

    result = make_night_time_zeros(start, end, gsp, gsp_yield_df, regime)

    assert len(result) == 36
    assert result["generation_mw"].sum() == 0
    assert result["capacity_mwp"][0] == 1.1
