import airflow
import unidecode
import git

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

START_DATE = airflow.utils.dates.days_ago(2)

default_args = {
    "owner": "raphael",
    "depends_on_past": False,
    "start_date": START_DATE,
    "email": ["lucas.freire@hdata.med.br"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=0),
    "provide_context": True,
}

GIT_DIR = '/home/hdata/hdata-rhp-airflow'

def git_pull():
    print("Atualizando projeto")
    g = git.cmd.Git(GIT_DIR)
    msg = g.pull()
    print(msg)


dag = DAG("pulling_code", default_args=default_args, schedule_interval='*/10 7-19 * * *')

t0 = PythonOperator(
    task_id="git_pull",
    python_callable=git_pull,
    dag=dag)

t0