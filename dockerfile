FROM python:3.9
RUN pip install wtforms==2.3.3 && \
    pip install 'apache-airflow[postgres]==1.10.14' && \
    pip install dbt==1.5.3 && \
    pip install SQLAlchemy==1.3.23

RUN mkdir /project
COPY scripts_airflow/ /project/scripts/

RUN chmod +x /project/scripts/init.sh
ENTRYPOINT [ "/project/scripts/init.sh" ]
