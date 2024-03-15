import airflow
import pandas as pd
import numpy as np
import datetime

# from utils.teams_robot import error_message
from datetime import timedelta
from dateutil import rrule
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from connections.oracle.connections import connect_rhp, connect_rhp_hdata
from queries.rhp.queries import *
from queries.rhp.queries_hdata import *

from utils.upsert_default import by_date_upsert_two_pk, by_date_upsert

START_DATE = airflow.utils.dates.days_ago(1)

default_args = {
    "owner": "Lucas",
    "depends_on_past": False,
    "start_date": START_DATE,
    "email": ["lucas.freire@eximio.med.br"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=0),
    "provide_context": True,
}

HOSPITAL = "REAL HOSPITAL PORTGUES"

def df_editor_clinico():
    print("Entrou no df_editor_clinico")
    # for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2023, 1, 1), until=dt_ontem):
    dt_ini = datetime.datetime(2023,1,1)
    for dt in rrule.rrule(rrule.DAILY, dtstart=dt_ini, until=dt_ontem):

        print(dt.strftime('%d/%m/%Y'), ' a ', dt.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_editor_clinico.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=dt.strftime('%d/%m/%Y')), connect_rhp())
        df_dim = df_dim.where(pd.notnull(df_dim), None)
        df_dim = df_dim.convert_dtypes()
        df_dim = df_dim.replace({np.nan: None})
        con = connect_rhp_hdata()
        cursor = con.cursor()
        if not df_dim.empty:
            list_cd_documento = list(df_dim["CD_EDITOR_CLINICO"])
            range_cd = int(len(list_cd_documento) / 999) + 1
            list_cds = [list_cd_documento[i::range_cd] for i in range(range_cd)]
            for cds in list_cds:
                cursor.execute('DELETE FROM MV_RHP.PW_EDITOR_CLINICO WHERE CD_EDITOR_CLINICO IN {cds}'.format(cds=tuple(cds)))
                con.commit()

        print("dados para incremento")
        print(df_dim.info())

        sql="INSERT INTO MV_RHP.PW_EDITOR_CLINICO (CD_EDITOR_CLINICO, CD_DOCUMENTO_CLINICO, CD_DOCUMENTO, CD_EDITOR_REGISTRO) VALUES (:1, :2, :3, :4)"

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

        print("Dados PW_EDITOR_CLINICO inseridos")

def df_editor_campo():
    print("Entrou no editor_campo")
    df_dim = pd.read_sql(query_editor_campo, connect_rhp())
    
    df_stage = pd.read_sql(query_editor_campo_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_CAMPO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.EDITOR_CAMPO (CD_CAMPO, DS_CAMPO) VALUES (:1, :2)"

    df_list = df_diff.values.tolist()
    n = 0
    cols = []
    for i in df_diff.iterrows():
        cols.append(df_list[n])
        n += 1
    cursor.executemany(sql, cols)
    con.commit()
    cursor.close
    con.close

    print("Dados editor_campo inseridos")

def df_registro_documento():
    print("Entrou no df_registro_documento")
    # for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.datetime(2023, 5, 13), until=dt_ontem):
    for dt in rrule.rrule(rrule.DAILY, dtstart=dt_ini, until=dt_ontem):
        print(dt.strftime('%d/%m/%Y'), ' a ', dt.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_registro_documento.format(dt=dt.strftime('%d/%m/%Y')), connect_rhp())
        df_dim = df_dim.where(pd.notnull(df_dim), None)
        df_dim = df_dim.convert_dtypes()
        df_dim = df_dim.replace({np.nan: None})
        df_dim['LO_VALOR'] = df_dim['LO_VALOR'][:4000]

        df_stage = pd.read_sql(query_registro_documento_hdata.format(dt=dt.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage["CD_REGISTRO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)
        print("dados para incremento")
        print(df_dim.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.REGISTRO_DOCUMENTO (CD_REGISTRO, SN_FECHADO, CD_CAMPO, DS_VALOR, LO_VALOR) \
            VALUES (:1, :2, :3, :4, :5)"

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

        print("Dados registro_documento inseridos")

def limit_lo_valor(df):
    df['LO_VALOR'] = df['LO_VALOR'][:4000]
    return df

dt_ontem = datetime.datetime.today() - datetime.timedelta(days=1)
dt_ini = dt_ontem - datetime.timedelta(days=5)

dag = DAG("captura_dados_extra_rhp", default_args=default_args, schedule_interval="0 4 * * *")

t1 = PythonOperator(
    task_id="captura_fech_chec_rhp",
    python_callable=by_date_upsert,
    op_kwargs={
        'query_origem': query_fech_chec,
        'tabela_destino': 'PW_HR_FECHADO_CHEC',
        'pk' : 'CD_FECHAMENTO_HORARIO_CHECAGEM',
        'inicio' : dt_ini,
        'fim' : dt_ontem
    },
    dag=dag
)

t2 = PythonOperator(
    task_id="captura_editor_clinico",
    python_callable=df_editor_clinico,
    dag=dag)

t3 = PythonOperator(
    task_id="captura_editor_campo",
    python_callable=df_editor_campo,
    dag=dag)

t4 = PythonOperator(
    task_id="captura_registro_documento",
    python_callable=by_date_upsert_two_pk,
    op_kwargs={
        'query_origem': query_registro_documento,
        'tabela_destino': 'REGISTRO_DOCUMENTO',
        'pk' : 'CD_REGISTRO',
        'pk2': 'CD_CAMPO',
        'inicio' : dt_ini,
        'fim' : dt_ontem,
        'mending_callback' : limit_lo_valor
    },
    dag=dag
)

t5 = PythonOperator(
    task_id="upsert_pw_encaminhamento",
    python_callable=by_date_upsert,
    op_kwargs={
        'inicio' : dt_ini,
        'fim' : dt_ontem,
        'query_origem' : query_encaminhamento_esp,
        'tabela_destino' : 'PW_ENCAMINHAMENTO',
        'pk' : 'CD_ENCAMINHAMENTO'},
    dag=dag
)

t6 = PythonOperator(
    task_id="upsert_new_prescr_check",
    python_callable=by_date_upsert_two_pk,
    op_kwargs={
        'query_origem': query_new_prescr_check,
        'tabela_destino': 'HRITPRE_CONS',
        'pk' : 'CD_ITPRE_MED',
        'pk2': 'DH_CHECAGEM',
        'inicio' : datetime.datetime(2022,1,1),
        'fim' : datetime.datetime(2022,6,30)},
    dag=dag
)

t1 >> t2 >> t3 >> t4 >> t5 >> t6