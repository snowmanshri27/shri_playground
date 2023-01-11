#!/usr/bin/env bash

# airflow home
export AIRFLOW_HOME=/Users/shri/Projects/airflow

# Setup DB Connection String
export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@localhost:5432/airflowdb"
# export AIRFLOW__CORE__SQL_ALCHEMY_CONN

AIRFLOW__WEBSERVER__SECRET_KEY="openssl rand -hex 30"
export AIRFLOW__WEBSERVER__SECRET_KEY

DBT_POSTGRESQL_CONN="postgresql+psycopg2://dbtuser:pssd@localhost:5432/dbtdb"

cd dbt && dbt compile
rm -f airflow/airflow-webserver.pid

sleep 10
airflow upgradedb
sleep 10
airflow connections --add --conn_id 'dbt_postgres_instance_raw_data' --conn_uri $DBT_POSTGRESQL_CONN
airflow scheduler & airflow webserver

# other airflow commands 
# airflow dags list
# airflow db check
# airflow db init
# 
