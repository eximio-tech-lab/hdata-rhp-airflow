import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from utils.teams_robot import error_message

START_DATE = airflow.utils.dates.days_ago(2)

default_args = {
    "owner": "Lucas R. Freire",
    "depends_on_past": False,
    "start_date": START_DATE,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=0),
    "provide_context": True,
}

def testing(**context):
    error_message("NOTIFICAÇÃO DE TESTE",
            ["lucas.freire@hdata.med.br","raphael.queiroz@hdata.med.br"],
            ["mensagem vinda do RHP",
            "sucesso"
            ],
            type='Stage')
    print('OK!')

dag = DAG("testing_dag", default_args=default_args, schedule_interval='10,15,20,25,30,35,40 17 * * *')

t0 = PythonOperator(
    task_id="testing",
    python_callable=testing,
    dag=dag
)

t0