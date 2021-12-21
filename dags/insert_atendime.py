import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from connections.oracle.connections import connect_rhp
from collections import OrderedDict as od
import unidecode

from queries.rhp.atendime import query_atendime


import pandas as pd
import numpy as np

START_DATE = airflow.utils.dates.days_ago(0)

default_args = {
    "owner": "raphael",
    "depends_on_past": False,
    "start_date": START_DATE,
    "email": ["raphael.queiroz@eximio.med.br"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=0),
    "provide_context": True,
}

HOSPITAL = 'REAL HOSPITAL PORTGUES'

def df_atendime():
    df = pd.read_sql(query_atendime, connect_rhp())

    print(df)

dag = DAG("insert_atendime_rhp", default_args=default_args, schedule_interval=None)

t0 = PythonOperator(
    task_id="insert_atendime_rhp",
    python_callable=df_atendime,
    op_kwargs={'connect_rhp': connect_rhp},
    on_failure_callback=None,
    dag=dag)

t0