import airflow
import pandas as pd
import datetime
from datetime import timedelta, date
from dateutil import rrule
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from utils.teams_robot import error_message
from queries.rhp.queries_temp import query_update_all_cid
from connections.oracle.connections import connect_rhp, connect_rhp_hdata

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

dt_ontem = datetime.datetime.today() - datetime.timedelta(days=1)

def testing(**context):
    error_message("NOTIFICAÇÃO DE TESTE",
            ["lucas.freire@hdata.med.br","raphael.queiroz@hdata.med.br"],
            ["mensagem vinda do RHP",
            "sucesso"],
            type='Stage')
    print('OK!')


def update_all_cid():
    for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.datetime(2022,1,1), until=dt_ontem):
        df_dim = pd.read_sql(query_update_all_cid.format(data=dt.strftime('%d/%m/%Y')), connect_rhp())
        print("dados para incremento")
        print(df_dim.info())

        con = connect_rhp_hdata()
        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.UPDATE_CID_TEMP (CD_ATENDIMENTO, CD_MULTI_EMPRESA, CD_CID) VALUES (:1, :2, :3)"

        df_list = df_dim.values.tolist()
        n = 0
        cols = []
        for i in df_dim.iterrows():
            cols.append(df_list[n])
            n += 1

        cursor.executemany(sql, cols)
        con.commit()
        cursor.close
        con.close
    error_message("CIDs ATUALIZADOS RHP",
            ["lucas.freire@hdata.med.br","raphael.queiroz@hdata.med.br"],
            ["mensagem vinda do RHP",
            "sucesso"],
            type='Stage')
    
    print("Dados inseridos")

dag = DAG("testing_dag", default_args=default_args, schedule_interval="0 5 * * *")

t0 = PythonOperator(
    task_id="update_all_cid",
    python_callable=update_all_cid,
    dag=dag
)

t0