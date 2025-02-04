FROM apache/airflow:2.9.2-python3.11
USER root
USER airflow
RUN pip install pymysql

COPY requirements.txt .
RUN pip install -r requirements.txt

