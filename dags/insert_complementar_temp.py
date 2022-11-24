import airflow
import unidecode
import pandas as pd
import numpy as np
import datetime

from datetime import timedelta, date
from dateutil import rrule
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from connections.oracle.connections_sml import connect_rhp, connect_rhp_hdata, engine_rhp, connect
# from connections.oracle.connections import connect_rhp, connect_rhp_hdata, engine_rhp, connect
from collections import OrderedDict as od
# from queries.rhp.queries import *
from queries.rhp.queries_temp import *
from queries.rhp.queries_hdata import *

from utils.integrity_checker import notify_email

START_DATE = airflow.utils.dates.days_ago(2)

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

HOSPITAL = "REAL HOSPITAL PORTGUES"

def update_cells(df_eq, table_name, CD):
    d = df_eq.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):

        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE {nome_tabela}\n'.format(nome_tabela=table_name)
        query += 'SET {nome_coluna} = CASE {cd}\n'.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if j[i + 1] is None:
                query += 'WHEN {cd_p_update} THEN null \n'.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                query += 'WHEN {cd_p_update} THEN {novo_valor}\n'.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                query += 'WHEN {cd_p_update} THEN {novo_valor}\n'.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna}\n'.format(nome_coluna=d['columns'][i + 1])
        query += 'END\n'
        query += 'WHERE {cd} IN({todos_cds}) and SK_REDE_HOSPITALAR IN (7, 8, 9);\n'.format(cd=CD, todos_cds=todos_cds)

        print(query)
        cursor.execute(query)
        conn.commit()
        conn.close()

def df_tempo_processo():
    print("Entrou no df_tempo_processo")
    for dt in rrule.rrule(rrule.DAILY, dtstart=dt_ini, until=dt_ontem):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(sacr_tempo_processo_others.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_rhp())

        df_dim["CD_TIPO_TEMPO_PROCESSO"] = df_dim["CD_TIPO_TEMPO_PROCESSO"].fillna(0)
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
        df_dim["NM_USUARIO"] = df_dim["NM_USUARIO"].fillna("0")

        df_stage = pd.read_sql(query_tempo_processo_hdata.format(data_ini=data_2.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.SACR_TEMPO_PROCESSO (DH_PROCESSO, CD_TIPO_TEMPO_PROCESSO, CD_ATENDIMENTO, NM_USUARIO) VALUES (:1, :2, :3, :4)"

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

        print("Dados SACR_TEMPO_PROCESSO inseridos")

        # df_upd = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
        # df_upd = df_upd.drop(columns=['_merge'])
        # df_upd = df_upd.reset_index(drop=True)

        # print("dados para update")
        # print(df_upd.info())

        # update_cells(df_upd, 'MV_RHP.SACR_TEMPO_PROCESSO', 'CD_ATENDIMENTO')

def df_documento_clinico():
    print("Entrou no df_documento_clinico")
    for dt in rrule.rrule(rrule.DAILY, dtstart=dt_ini, until=dt_ontem):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_documento_clinico_others.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_rhp())

        df_dim["CD_OBJETO"] = df_dim["CD_OBJETO"].fillna(0)
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
        df_dim["CD_TIPO_DOCUMENTO"] = df_dim["CD_TIPO_DOCUMENTO"].fillna(0)
        df_dim["TP_STATUS"] = df_dim["TP_STATUS"].fillna("0")

        df_stage = pd.read_sql(query_documento_clinico_hdata.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage["CD_OBJETO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.PW_DOCUMENTO_CLINICO (CD_OBJETO, CD_ATENDIMENTO, CD_TIPO_DOCUMENTO, TP_STATUS, DH_CRIACAO) VALUES (:1, :2, :3, :4, :5)"

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

        print("Dados PW_DOCUMENTO_CLINICO inseridos")

        df_upd = df_dim[df_dim['CD_OBJETO'].isin(df_stage['CD_OBJETO'])]

        print("dados para update")
        print(df_upd.info())

        # if not df_upd.empty:

        #     update_cells(df_upd, 'MV_RHP.PW_DOCUMENTO_CLINICO', 'CD_OBJETO')

dt_ontem = datetime.datetime.today() - datetime.timedelta(days=1)
dt_ini = dt_ontem - datetime.timedelta(days=7)
dt_ini = datetime.datetime(2019, 1, 1)
dt_ontem = datetime.datetime(2022, 11, 22)

dag = DAG("insert_complementar_temp", default_args=default_args, schedule_interval=None)

t0 = PythonOperator(
    task_id="insert_tempo_processo_rhp",
    python_callable=df_tempo_processo,
    dag=dag)

t1 = PythonOperator(
    task_id="insert_documento_clinico_rhp",
    python_callable=df_documento_clinico,
    dag=dag)

t0 >> t1