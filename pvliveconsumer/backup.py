"""Create gsp data from nataionl yield."""

import logging
from datetime import datetime

from nowcasting_datamodel.models.gsp import GSPYieldSQL, LocationSQL
from nowcasting_datamodel.read.read_gsp import get_gsp_yield
from sqlalchemy import func
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def get_number_gsp_yields(
    start_datetime_utc: datetime,
    end_datetime_utc: datetime,
    session: Session,
    regime: str | None = None,
) -> int:
    """Get the numner of gsp yields.

    :param start_datetime_utc: start time to filter on
    :param end_datetime_utc: end time to filter on
    :param session: database session
    :param regime: regim
    :return: the number of gsp yields in that regime
    """
    query = session.query(func.count(GSPYieldSQL.id))
    query = query.join(LocationSQL)
    query = query.where(LocationSQL.id == GSPYieldSQL.location_id)

    # filter on regime
    if regime is not None:
        query = query.where(GSPYieldSQL.regime == regime)

    # filter on time
    query = query.where(GSPYieldSQL.datetime_utc >= start_datetime_utc)
    query = query.where(GSPYieldSQL.datetime_utc <= end_datetime_utc)

    # dont include national
    query = query.where(LocationSQL.gsp_id != 0)

    n_gsp_yields_sql = query.all()[0][0]

    logger.debug(
        f"Found {n_gsp_yields_sql} GSP yields from "
        f"{start_datetime_utc} to {end_datetime_utc}, not including national",
    )

    return n_gsp_yields_sql


def make_gsp_yields_from_national(
    session: Session,
    start: datetime,
    end: datetime,
    regime: str,
    locations: list[LocationSQL],
) -> list[GSPYieldSQL]:
    """Make gsp yields from national.

    :param session:
    :param start:
    :param end:
    :param locations:
    :return:
    """
    logger.info("Make GSP yields from national if needed")

    # 1. check number of gsps
    n_gsp_yeilds_sql = get_number_gsp_yields(
        start_datetime_utc=start,
        end_datetime_utc=end,
        session=session,
    )
    if n_gsp_yeilds_sql > 0:
        logger.debug(
            "Will not interpolate GSP results as there are already GSP results in the database",
        )
        return []

    # 2. load national results for the last hour
    national_gsp_yields = get_gsp_yield(
        session=session,
        gsp_ids=[0],
        start_datetime_utc=start,
        regime=regime,
        end_datetime_utc=end,
    )
    logger.debug(
        f"Found {len(national_gsp_yields)} naional yields from {start} to {end} for {regime=}",
    )

    # 3. make gsps value from national, scalling by capacity
    gsp_yields = []
    for national_gsp_yield in national_gsp_yields:
        for location in locations:
            if location.gsp_id != 0:
                if location.installed_capacity_mw is not None:
                    factor = (
                        location.installed_capacity_mw
                        / national_gsp_yield.location.installed_capacity_mw
                    )
                else:
                    factor = 1 / national_gsp_yield.location.installed_capacity_mw
                logger.debug(f"National to GSP factor for gsp id {location.gsp_id} is {factor}")

                gsp_yield = GSPYieldSQL(
                    datetime_utc=national_gsp_yield.datetime_utc,
                    solar_generation_kw=national_gsp_yield.solar_generation_kw * factor,
                    regime=national_gsp_yield.regime,
                    capacity_mwp=location.installed_capacity_mw,
                    pvlive_updated_utc=national_gsp_yield.pvlive_updated_utc,
                )
                gsp_yield.location = location
                gsp_yields.append(gsp_yield)

    logger.debug(f"Made {len(gsp_yields)} extra gsp yields by using the national value")

    return gsp_yields
