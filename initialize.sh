# !/bin/bash

mkdir -p ./dags ./logs ./plugins ./config
chmod -R 777 config
chmod -R 777 logs
chmod -R 777 plugins
echo -e "AIRFLOW_UID=$(id -u)" >.env
docker-compose up airflow-init
