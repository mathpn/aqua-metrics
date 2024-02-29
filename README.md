# Aqua Metrics

The goal of this project is to learn about Apache Airflow. Data from the [National Data Buoy Center](https://www.ndbc.noaa.gov/) will be ingested and processed. Then, a dashboard will present the data.

## Running the service

### Running (production mode)

```bash
docker compose up
```

### Rebuilding the Docker images before running

```bash
docker compose rm && docker compose up --build
```
