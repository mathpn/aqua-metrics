from sqlalchemy import (
    create_engine,
    Table,
    MetaData,
    Column,
    ForeignKey,
    String,
    Integer,
    Float,
    DateTime,
)


URI = "sqlite:///./data/database.db"


def main():
    engine = create_engine(URI)
    engine = create_engine(URI, echo=True)

    metadata_obj = MetaData()

    stations_table = Table(
        "stations",
        metadata_obj,
        Column("station_code", String(16), primary_key=True),
        Column("name", String(32)),
        Column("lat", Float),
        Column("lon", Float),
    )

    realtime_table = Table(
        "realtime_data",
        metadata_obj,
        Column("station_code", ForeignKey("stations.station_code"), nullable=False),
        Column("WDIR", Integer),
        Column("WSPD", Float),
        Column("GST", Float),
        Column("WVHT", Float),
        Column("DPD", Float),
        Column("APD", Float),
        Column("MWD", Float),
        Column("PRES", Float),
        Column("PTDY", Float),
        Column("ATMP", Float),
        Column("WTMP", Float),
        Column("DEWP", Float),
        Column("VIS", Float),
        Column("TIDE", Float),
        Column("timestamp", DateTime),
        Column("ingestion_ts", DateTime),
    )

    metadata_obj.create_all(engine)


if __name__ == "__main__":
    main()
