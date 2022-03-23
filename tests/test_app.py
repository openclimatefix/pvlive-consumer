
from click.testing import CliRunner
from nowcasting_datamodel.models.gsp import Location, LocationSQL, GSPYieldSQL

from gspconsumer.app import app, pull_data_and_save


def test_pull_data(db_session):

    gsps = [
        Location(gsp_id=1, label="GSP_0").to_orm(),
    ]
    gsps[0].last_gsp_yield = None

    pull_data_and_save(gsps=gsps, session=db_session)

    pv_yields = db_session.query(GSPYieldSQL).all()
    assert len(pv_yields) > 0


def test_app(db_connection):

    runner = CliRunner()
    response = runner.invoke(app, ["--db-url", db_connection.url, "--n-gsps", 10])
    assert response.exit_code == 0, response.exception

    with db_connection.get_session() as session:
        gsps = session.query(LocationSQL).all()
        _ = Location.from_orm(gsps[0])
        assert len(gsps) == 10

        gsp_yields = session.query(GSPYieldSQL).all()
        assert len(gsp_yields) > 9


def test_app_day_after(db_connection):

    runner = CliRunner()
    response = runner.invoke(app, ["--db-url", db_connection.url, "--n-gsps", 10, "--regime", 'day-after'])
    assert response.exit_code == 0, response.exception

    with db_connection.get_session() as session:
        gsps = session.query(LocationSQL).all()
        _ = Location.from_orm(gsps[0])
        assert len(gsps) == 10

        gsp_yields = session.query(GSPYieldSQL).all()
        assert len(gsp_yields) == 10 * 48  # 10 gsps with 48 hour settlement periods