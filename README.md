# Aqua Metrics

The goal of this project is to learn about Apache Airflow. Data from the [National Data Buoy Center](https://www.ndbc.noaa.gov/) (NDBC) is extracted and processed. A simple dashboard displays information extracted from the data.

## How does it work?

Apache Airflow is used to orchestrate the required workflows. Two sources of data are used:

- Real-time data for all stations
- Historical data (last 45 days) for each station

Both are publicly accessible via the NDBC website. Two PostgreSQL instances are used, one as the database backend for Airflow and the other as the database to store all data. In the latter, all the required tables are created when starting the PostgreSQL Docker (check [here](./sql/create_tables.sql)).

There are three DAGs, namely:

- _fetch_realtime_: extracts real-time data from the NDBC website, register all stations in the stations table, and fills a table with the latest real-time observations
- _fetch_history_: extracts historical data (last 45 days) for all known stations and fills a table with the latest historical data for each station
- _atmp_zscore_: TODO


## Running the service

### Running (production mode)

```bash
docker compose up
```

### Rebuilding the Docker images before running

```bash
docker compose rm && docker compose up --build
```
