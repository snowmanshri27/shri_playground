# Airflow-DBT Use Case on top of Instacart order data

This is a simple end to end use case that loads sample orders made by Instacart users. End to end refers the ETL stages from sources -> metrics. The source is loaded using Airflow. The rest of the stages are built through DBT with a combination of tables, views and metrics at the very end using Metric Flow. The whole flow is explained in detail here: https://docs.google.com/document/d/14T-ALMluHXcLacwurJiyAcfDGp_1AME2wBAYlkVb-gA/edit#heading=h.5e11yvub20v0
