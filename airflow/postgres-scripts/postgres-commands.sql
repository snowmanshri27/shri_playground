ALTER ROLE airflow SET search_path TO airflow;
-- ALTER ROLE airflow SET search_path TO airflowdb;
-- ALTER ROLE airflow SET search_path TO postgres;
-- ALTER ROLE shri SET search_path TO shri;

CREATE SCHEMA IF NOT EXISTS airflow AUTHORIZATION airflow;

SET datestyle = "ISO, DMY";