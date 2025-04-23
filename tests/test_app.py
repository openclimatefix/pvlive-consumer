from click.testing import CliRunner
from datetime import datetime, timedelta
from nowcasting_datamodel.models.gsp import GSPYieldSQL, Location, LocationSQL
from nowcasting_datamodel.models.models import national_gb_label

from pvliveconsumer.app import app, pull_data_and_save

from freezegun import freeze_time


def make_national(db_connection):
    gsps = [
        Location(gsp_id=0, label=national_gb_label, installed_capacity_mw=10).to_orm(),
    ]
    with db_connection.get_session() as session:
        session.add_all(gsps)
        session.commit()


def test_pull_data(db_session, input_data_last_updated_sql):
    gsps = [
        Location(gsp_id=0, label="GSP_0", installed_capacity_mw=10).to_orm(),
    ]
    gsps[0].last_gsp_yield = None

    pull_data_and_save(gsps=gsps, session=db_session)

    pv_yields = db_session.query(GSPYieldSQL).all()
    assert len(pv_yields) > 0
    assert pv_yields[0].pvlive_updated_utc != None
    assert pv_yields[0].capacity_mwp != None

    gsps = db_session.query(LocationSQL).all()
    assert gsps[0].installed_capacity_mw != 10


tomorrow_date = (datetime.today() + timedelta(days=1)).date()


@freeze_time(tomorrow_date)
def test_pull_data_night_time(db_session):
    gsps = [
        Location(gsp_id=1, label="GSP_1", installed_capacity_mw=10).to_orm(),
    ]
    gsps[0].last_gsp_yield = None

    pull_data_and_save(gsps=gsps, session=db_session)

    pv_yields = db_session.query(GSPYieldSQL).all()
    assert len(pv_yields) > 0
    assert pv_yields[0].pvlive_updated_utc != None
    assert pv_yields[0].capacity_mwp != None
    assert pv_yields[0].solar_generation_kw == 0


def test_app(db_connection, input_data_last_updated_sql):
    make_national(db_connection)

    runner = CliRunner()
    response = runner.invoke(app, ["--db-url", db_connection.url, "--n-gsps", 10])
    assert response.exit_code == 0, response.exception

    with db_connection.get_session() as session:
        gsps = session.query(LocationSQL).all()
        _ = Location.from_orm(gsps[0])
        assert len(gsps) == 11

        gsp_yields = session.query(GSPYieldSQL).order_by(GSPYieldSQL.datetime_utc).all()
        assert len(gsp_yields) >= 11


@freeze_time("2025-04-21 12:00:00")
def test_app_day_after(db_connection, input_data_last_updated_sql):
    make_national(db_connection)

    runner = CliRunner()
    n_gsps = 4
    response = runner.invoke(
        app, ["--db-url", db_connection.url, "--n-gsps", n_gsps, "--regime", "day-after"]
    )
    assert response.exit_code == 0, response.exception

    with db_connection.get_session() as session:
        gsps = session.query(LocationSQL).all()
        _ = Location.from_orm(gsps[0])
        assert len(gsps) == n_gsps + 1

        gsp_yields = session.query(GSPYieldSQL).all()
        assert len(gsp_yields) == (n_gsps + 1) * 49  # (10 +national) gsps with
        # 8 half hour settlement periods + midnight


@freeze_time("2025-04-21 12:00:00")
def test_app_day_after_national_only(db_connection, input_data_last_updated_sql):
    make_national(db_connection)

    runner = CliRunner()
    response = runner.invoke(
        app, ["--db-url", db_connection.url, "--n-gsps", 0, "--regime", "day-after"]
    )
    assert response.exit_code == 0, response.exception

    with db_connection.get_session() as session:
        gsps = session.query(LocationSQL).all()
        _ = Location.from_orm(gsps[0])
        assert len(gsps) == 1

        gsp_yields = session.query(GSPYieldSQL).all()
        assert len(gsp_yields) == 1 * 49  # 1 gsps with 48 half hour settlement periods + midnight


@freeze_time("2025-04-21 12:00:00")
def test_app_day_after_gsp_only(db_connection, input_data_last_updated_sql):
    runner = CliRunner()
    response = runner.invoke(
        app,
        [
            "--db-url",
            db_connection.url,
            "--n-gsps",
            4,
            "--regime",
            "day-after",
            "--include-national",
            "false",
        ],
    )
    assert response.exit_code == 0, response.exception

    with db_connection.get_session() as session:
        gsps = session.query(LocationSQL).all()
        _ = Location.from_orm(gsps[0])
        assert len(gsps) == 4

        gsp_yields = session.query(GSPYieldSQL).all()
        for gsp in gsp_yields:
            print(gsp.__dict__)
        assert len(gsp_yields) == 4 * 49  # 4 gsps with 48 half hour settlement periods + midnight


@freeze_time("2025-04-21 12:00:00")
def test_app_day_after_gsp_only_after_national(db_connection, input_data_last_updated_sql):
    """
    First just get National, then get all gsps
    """

    make_national(db_connection)

    runner = CliRunner()
    response = runner.invoke(
        app, ["--db-url", db_connection.url, "--n-gsps", 0, "--regime", "day-after"]
    )
    assert response.exit_code == 0, response.exception

    response = runner.invoke(
        app,
        [
            "--db-url",
            db_connection.url,
            "--n-gsps",
            4,
            "--regime",
            "day-after",
            "--include-national",
            "true",
        ],
    )
    assert response.exit_code == 0, response.exception

    with db_connection.get_session() as session:
        gsps = session.query(LocationSQL).all()
        _ = Location.from_orm(gsps[0])
        assert len(gsps) == 5

        gsp_yields = session.query(GSPYieldSQL).all()
        assert len(gsp_yields) == 5 * 49
        # national + 4 gsps with 48 half hour settlement periods + midnight
