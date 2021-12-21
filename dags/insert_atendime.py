import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from connections.oracle.connections import connect_rhp, connect_rhp_2
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
    print("Entrou no df_atendime")

    df = pd.read_sql(query_atendime, connect_rhp())

    print(df)

def df_cid():
    print("Entrou no df_cid")

    df = pd.read_sql(query_cid, connect_rhp())

    print(df)

def df_classificacao_risco():
    print("Entrou no df_classificacao_risco")

    df = pd.read_sql(query_classificacao_risco, connect_rhp())

    print(df)

def df_classificacao():
    print("Entrou no df_classificacao")

    df = pd.read_sql(query_classificacao, connect_rhp())

    print(df)

def df_convenio():
    print("Entrou no df_convenio")

    df = pd.read_sql(query_convenio, connect_rhp())

    print(df)

dag = DAG("insert_dados_rhp", default_args=default_args, schedule_interval=None)

t0 = PythonOperator(
    task_id="insert_atendime_rhp",
    python_callable=df_atendime,
    dag=dag)

t1 = PythonOperator(
    task_id="insert_cid_rhp",
    python_callable=df_cid,
    dag=dag)

t2 = PythonOperator(
    task_id="insert_classificacao_risco_rhp",
    python_callable=df_classificacao_risco,
    dag=dag)

t3 = PythonOperator(
    task_id="insert_classificacao_rhp",
    python_callable=df_classificacao,
    dag=dag)

t4 = PythonOperator(
    task_id="insert_convenio_rhp",
    python_callable=df_convenio,
    dag=dag)

t0 >> t1 >> t2 >> t3 >> t4