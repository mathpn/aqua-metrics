from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import MetaData, Table, create_engine, select


@task()
def list_stations():
    uri = SqliteHook(sqlite_conn_id="aqua_metrics_sqlite").get_uri()
    engine = create_engine(uri)

    metadata = MetaData()
    table = Table("stations", metadata, autoload=True, autoload_with=engine)
    stmt = select(table.c.station_code)

    with engine.connect() as conn:
        return [row[0] for row in conn.execute(stmt)]
