import airflow
import os
import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

args = {"owner": "airflow"}

default_args = {
    "owner": "airflow",
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'schedule': "@once",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# instantiating the Postgres Operator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "postgres_operator_example_dag"

dag_psql = DAG(
    dag_id=DAG_ID,
    default_args=args,
    # schedule_interval='0 0 * * *',
    schedule_interval="@once",
    dagrun_timeout=timedelta(minutes=60),
    description="use case of psql operator in airflow",
    start_date=airflow.utils.dates.days_ago(1),
)

create_pet_table = PostgresOperator(
    task_id="create_pet_table",
    postgres_conn_id="dbt_postgres_instance_raw_data",
    sql="sql/create_pet.sql",
    dag=dag_psql,
)

populate_pet_table = PostgresOperator(
    task_id="populate_pet_table",
    postgres_conn_id="dbt_postgres_instance_raw_data",
    sql="sql/populate_pet.sql",
)

get_all_pets = PostgresOperator(
    task_id="get_all_pets",
    postgres_conn_id="dbt_postgres_instance_raw_data",
    sql="SELECT * FROM pet;",
)

# get_birth_date = PostgresOperator(
#     task_id="get_birth_date",
#     sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
#     parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
#     runtime_parameters={"statement_timeout": "3000ms"},
# )

create_pet_table >> populate_pet_table >> get_all_pets
# >> get_birth_date

if __name__ == "__main__":
    dag_psql.cli()
