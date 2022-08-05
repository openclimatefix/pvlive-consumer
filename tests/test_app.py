from click.testing import CliRunner
from nowcasting_datamodel.models.gsp import GSPYieldSQL, Location, LocationSQL

from gspconsumer.app import app, pull_data_and_save


def test_pull_data(db_session, input_data_last_updated_sql):

    gsps = [
        Location(gsp_id=0, label="GSP_0").to_orm(),
    ]
    gsps[0].last_gsp_yield = None

    pull_data_and_save(gsps=gsps, session=db_session)

    pv_yields = db_session.query(GSPYieldSQL).all()
    assert len(pv_yields) > 0


def test_app(db_connection, input_data_last_updated_sql):

    runner = CliRunner()
    response = runner.invoke(app, ["--db-url", db_connection.url, "--n-gsps", 10])
    assert response.exit_code == 0, response.exception

    with db_connection.get_session() as session:
        gsps = session.query(LocationSQL).all()
        _ = Location.from_orm(gsps[0])
        assert len(gsps) == 11

        gsp_yields = session.query(GSPYieldSQL).all()
        assert len(gsp_yields) > 9


def test_app_day_after(db_connection, input_data_last_updated_sql):

    runner = CliRunner()
    response = runner.invoke(
        app, ["--db-url", db_connection.url, "--n-gsps", 10, "--regime", "day-after"]
    )
    assert response.exit_code == 0, response.exception

    with db_connection.get_session() as session:
        gsps = session.query(LocationSQL).all()
        _ = Location.from_orm(gsps[0])
        assert len(gsps) == 11

        gsp_yields = session.query(GSPYieldSQL).all()
        assert len(gsp_yields) == 11 * 48  # (10 +national) gsps with 48 hour settlement periods


def test_app_day_after_national_only(db_connection, input_data_last_updated_sql):

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
        assert len(gsp_yields) == 1 * 48  # 1 gsps with 48 hour settlement periods


def test_app_day_after_gsp_only(db_connection, input_data_last_updated_sql):

    runner = CliRunner()
    response = runner.invoke(
        app, ["--db-url", db_connection.url, "--n-gsps", 5, "--regime", "day-after","--include-national", "false"]
    )
    assert response.exit_code == 0, response.exception

    with db_connection.get_session() as session:
        gsps = session.query(LocationSQL).all()
        _ = Location.from_orm(gsps[0])
        assert len(gsps) == 5

        gsp_yields = session.query(GSPYieldSQL).all()
        assert len(gsp_yields) == 5* 48  # 1 gsps with 48 hour settlement periods
