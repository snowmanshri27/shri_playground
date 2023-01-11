from airflow import utils
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from datetime import timedelta

# [START default_args]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'start_date': datetime(2023, 1, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}
# [END default_args]
DAG_ID = "load_instacart_data"

# [START instantiate_dag]
load_instacart_data = DAG(
    dag_id = DAG_ID,
    default_args=default_args,
    schedule_interval = None,
    dagrun_timeout=timedelta(minutes=60),
    description='use case of psql operator in airflow',
    start_date = utils.dates.days_ago(1)
)

start = DummyOperator(
    task_id='start',
    dag=load_instacart_data,
)

t1 = PostgresOperator(
        task_id='drop_table_aisles',
        sql="DROP TABLE IF EXISTS aisles;",
        postgres_conn_id="dbt_postgres_instance_raw_data",
        autocommit=True,
        dag=load_instacart_data,
    )

t2 = PostgresOperator(
        task_id='create_aisles',
        sql="create table if not exists aisles (aisle_id integer, aisle varchar(100));",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data,
    )

t3 = PostgresOperator(
        task_id='load_aisles',
        sql="COPY aisles FROM '/sample_data/aisles.csv' DELIMITER ',' CSV HEADER;",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        database="dbtdb",
        dag=load_instacart_data,
    )

t4 = PostgresOperator(
        task_id='drop_table_departments',
        sql="DROP TABLE IF EXISTS departments;",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data,
    )

t5 = PostgresOperator(
        task_id='create_departments',
        sql="create table if not exists departments (department_id integer, department varchar(100));",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data,
    )

t6 = PostgresOperator(
        task_id='load_departments',
        sql="	COPY departments FROM '/sample_data/departments.csv' DELIMITER ',' CSV HEADER;",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data,
    )                     

t7 = PostgresOperator(
        task_id='drop_table_products',
        sql="DROP TABLE IF EXISTS aisles;",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data,
    )

t8 = PostgresOperator(
        task_id='create_products',
        sql="create table if not exists products (product_id integer, product_name varchar(200),	aisle_id integer, department_id integer);",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data,
    )

t9 = PostgresOperator(
        task_id='load_products',
        sql="	COPY products FROM '/sample_data/products.csv' DELIMITER ',' CSV HEADER;",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data,
    )  

t10 = PostgresOperator(
        task_id='drop_table_orders',
        sql="DROP TABLE IF EXISTS orders;",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data,
    )

t11 = PostgresOperator(
        task_id='create_orders',
        sql="create table if not exists orders ( order_id integer,user_id integer, eval_set varchar(10), order_number integer,order_dow integer,order_hour_of_day integer, days_since_prior_order real);",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data,
    )

t12 = PostgresOperator(
        task_id='load_orders',
        sql="	COPY orders FROM '/sample_data/orders.csv' DELIMITER ',' CSV HEADER;",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data,
    ) 

t13 = PostgresOperator(
        task_id='drop_table_order_products__prior',
        sql="DROP TABLE IF EXISTS order_products__prior;",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data,
    )

t14 = PostgresOperator(
        task_id='create_order_products__prior',
        sql="create table if not exists order_products__prior(order_id integer, product_id integer, add_to_cart_order integer, reordered integer);",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data
    )

t15 = PostgresOperator(
        task_id='load_order_products__prior',
        sql="	COPY order_products__prior FROM '/sample_data/order_products__prior.csv' DELIMITER ',' CSV HEADER;",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data
    )   

t16 = PostgresOperator(
        task_id='drop_table_order_products__train',
        sql="DROP TABLE IF EXISTS order_products__train;",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data
    )

t17 = PostgresOperator(
        task_id='create_order_products__train',
        sql="create table if not exists order_products__train(order_id integer, product_id integer, add_to_cart_order integer, reordered integer);",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data
    )

t18 = PostgresOperator(
        task_id='load_order_products__train',
        sql="	COPY order_products__train FROM '/sample_data/order_products__train.csv' DELIMITER ',' CSV HEADER;",
        postgres_conn_id='dbt_postgres_instance_raw_data',
        autocommit=True,
        dag=load_instacart_data
    )       

start >> t1 >> t2 >> t3 
start >> t4 >> t5 >> t6 
start >> t7 >> t8 >> t9
start >> t10 >> t11 >> t12
start >> t13 >> t14 >> t15
start >> t16 >> t17 >> t18

if __name__ == "__main__":
    load_instacart_data.cli()