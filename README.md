# Aqua Metrics

Aqua Metrics is a learning project designed to explore the capabilities of [Apache Airflow](https://airflow.apache.org/). Our goal is to extract and process data from the [National Data Buoy Center](https://www.ndbc.noaa.gov/) (NDBC), presenting it through an intuitive dashboard.

## How it works

Apache Airflow is used to orchestrate the required workflows. The project utilizes two key data sources:

- Real-time data for all stations
- Historical data (last 45 days) for each station

Accessible via the NDBC website, these datasets are managed by two PostgreSQL instances. One serves as the database backend for Airflow, while the other stores all extracted data. Upon initiating the PostgreSQL Docker (details in [create_tables.sql](./sql/create_tables.sql)), the necessary tables are created.

There are three DAGs, namely:

- _fetch_realtime_: Extracts real-time data, registers all stations in the stations table, and populates a table with the latest observations.
- _fetch_history_: Retrieves historical data for all known stations over the past 45 days, updating a dedicated table.
- _anomaly_zscore_: Leveraging historical data, certain metrics (e.g., atmospheric temperature) undergo deseasonalization, detrending, and transformation into z-scores. The resulting z-score is stored, indicating the anomaly level of the latest measurement.

A user-friendly dashboard, created with [Streamlit](https://streamlit.io/) displays the insights generated through these DAGs.

## Running the Service

### Running with Docker

```bash
docker compose up
```

### Rebuilding the Docker Images

```bash
docker compose rm && docker compose up --build
```

## Running without Docker

For those preferring a virtual Python environment, install all requirements. Note that Apache Airflow is _not in the requirements file_ since the Airflow Docker image is used and _must be installed separately_.
