import airflow
import unidecode
import pandas as pd
import numpy as np
import datetime

from datetime import timedelta, date
from dateutil import rrule
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from connections.oracle.connections import connect_rhp, connect_rhp_hdata, engine_rhp, connect
# from connections.oracle.connections_sml import connect_rhp, connect_rhp_hdata, engine_rhp, connect
from collections import OrderedDict as od
from queries.rhp.queries import *
from queries.rhp.queries_hdata import *


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

HOSPITAL = "REAL HOSPITAL PORTGUES"

def df_atendime():
    print("Entrou no df_atendime")
    print(dt_back.strftime('%d/%m/%Y'), ' a ', dt.strftime('%d/%m/%Y'))

    df_dim = pd.read_sql(query_atendime.format(data_ini=dt_back.strftime('%d/%m/%Y'), data_fim=dt.strftime('%d/%m/%Y')), connect_rhp())

    df_dim["CD_MULTI_EMPRESA"] = df_dim["CD_MULTI_EMPRESA"].fillna(0)
    df_dim["CD_PACIENTE"] = df_dim["CD_PACIENTE"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_CID"] = df_dim["CD_CID"].fillna("0")
    df_dim["CD_MOT_ALT"] = df_dim["CD_MOT_ALT"].fillna(0)
    df_dim["CD_TIP_RES"] = df_dim["CD_TIP_RES"].fillna(0)
    df_dim["CD_CONVENIO"] = df_dim["CD_CONVENIO"].fillna(0)
    df_dim["CD_ESPECIALID"] = df_dim["CD_ESPECIALID"].fillna(0)
    df_dim["CD_PRESTADOR"] = df_dim["CD_PRESTADOR"].fillna(0)
    df_dim["CD_ATENDIMENTO_PAI"] = df_dim["CD_ATENDIMENTO_PAI"].fillna(0)
    df_dim["CD_LEITO"] = df_dim["CD_LEITO"].fillna(0)
    df_dim["CD_ORI_ATE"] = df_dim["CD_ORI_ATE"].fillna(0)
    df_dim["CD_SERVICO"] = df_dim["CD_SERVICO"].fillna(0)
    df_dim["TP_ATENDIMENTO"] = df_dim["TP_ATENDIMENTO"].fillna("0")
    df_dim["CD_TIP_MAR"] = df_dim["CD_TIP_MAR"].fillna(0)
    df_dim["CD_SINTOMA_AVALIACAO"] = df_dim["CD_SINTOMA_AVALIACAO"].fillna(0)
    df_dim["NM_USUARIO_ALTA_MEDICA"] = df_dim["NM_USUARIO_ALTA_MEDICA"].fillna("0")

    df_dim['HR_ALTA'] = df_dim['HR_ALTA'].astype(str)
    df_dim['HR_ALTA_MEDICA'] = df_dim['HR_ALTA_MEDICA'].astype(str)

    lista_cds_atendimentos = df_dim['CD_ATENDIMENTO'].to_list()
    lista_cds_atendimentos = [str(cd) for cd in lista_cds_atendimentos]
    atendimentos = ','.join(lista_cds_atendimentos)

    print(df_dim.info())

    df_stage = pd.read_sql(query_atendime_hdata.format(data_ini=dt_back.strftime('%d/%m/%Y'), data_fim=dt.strftime('%d/%m/%Y')), connect_rhp_hdata())

    df_stage['HR_ALTA'] = df_stage['HR_ALTA'].astype(str)
    df_stage['HR_ALTA_MEDICA'] = df_stage['HR_ALTA_MEDICA'].astype(str)

    print(df_stage.info())

    df_diff = df_dim.merge(df_stage["CD_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    df_diff['HR_ALTA'] = pd.to_datetime(df_diff['HR_ALTA'])
    df_diff['HR_ALTA_MEDICA'] = pd.to_datetime(df_diff['HR_ALTA_MEDICA'])

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()
    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ATENDIME (CD_MULTI_EMPRESA, CD_PACIENTE, CD_ATENDIMENTO, CD_CID, CD_MOT_ALT, CD_TIP_RES, CD_CONVENIO, CD_ESPECIALID, CD_PRESTADOR, CD_ATENDIMENTO_PAI, CD_LEITO, CD_ORI_ATE, CD_SERVICO, TP_ATENDIMENTO, DT_ATENDIMENTO, HR_ATENDIMENTO, HR_ALTA, HR_ALTA_MEDICA, CD_TIP_MAR, CD_SINTOMA_AVALIACAO, NM_USUARIO_ALTA_MEDICA) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21)"

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

    print("Dados ATENDIME inseridos")

    df_upd = df_dim.merge(df_stage["CD_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    df_upd['HR_ALTA'] = pd.to_datetime(df_upd['HR_ALTA'])
    df_upd['HR_ALTA_MEDICA'] = pd.to_datetime(df_upd['HR_ALTA_MEDICA'])

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.ATENDIME '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_ATENDIMENTO', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

    df_diagnostico_atendime(atendimentos)

def df_cid():
    print("Entrou no df_cid")

    df_dim = pd.read_sql(query_cid, connect_rhp())

    df_stage = pd.read_sql(query_cid_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.CID (CD_CID, DS_CID, CD_SGRU_CID) VALUES (:1, :2, :3)"

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

    print("Dados CID inseridos")

    df_upd = df_dim.merge(df_stage["CD_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.CID '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_CID', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_classificacao_risco():
    print("Entrou no df_classificacao_risco")
    print(dt_back.strftime('%d/%m/%Y'), ' a ', dt.strftime('%d/%m/%Y'))

    df_dim = pd.read_sql(query_classificacao_risco.format(data_ini=dt_back.strftime('%d/%m/%Y'), data_fim=dt.strftime('%d/%m/%Y')), connect_rhp())

    df_dim["CD_CLASSIFICACAO_RISCO"] = df_dim["CD_CLASSIFICACAO_RISCO"].fillna(0)
    df_dim["CD_COR_REFERENCIA"] = df_dim["CD_COR_REFERENCIA"].fillna(0)
    df_dim["CD_TRIAGEM_ATENDIMENTO"] = df_dim["CD_TRIAGEM_ATENDIMENTO"].fillna(0)

    df_stage = pd.read_sql(query_classificacao_risco_hdata.format(data_ini=dt_back.strftime('%d/%m/%Y'), data_fim=dt.strftime('%d/%m/%Y')), connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_CLASSIFICACAO_RISCO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.SACR_CLASSIFICACAO_RISCO (CD_CLASSIFICACAO_RISCO, CD_COR_REFERENCIA, CD_TRIAGEM_ATENDIMENTO, DH_CLASSIFICACAO_RISCO, CD_CLASSIFICACAO) VALUES (:1, :2, :3, :4, :5)"

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

    print("Dados SACR_CLASSIFICACAO_RISCO inseridos")

    df_upd = df_dim.merge(df_stage["CD_CLASSIFICACAO_RISCO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.SACR_CLASSIFICACAO_RISCO '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_CLASSIFICACAO_RISCO', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_classificacao():
    print("Entrou no df_classificacao")

    df_dim = pd.read_sql(query_classificacao, connect_rhp())

    df_stage = pd.read_sql(query_classificacao_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_CLASSIFICACAO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.SACR_CLASSIFICACAO (CD_CLASSIFICACAO, DS_TIPO_RISCO, CD_COR_REFERENCIA) VALUES (:1, :2, :3)"

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

    print("Dados SACR_CLASSIFICACAO inseridos")

    df_upd = df_dim.merge(df_stage["CD_CLASSIFICACAO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.SACR_CLASSIFICACAO '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_CLASSIFICACAO', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_convenio():
    print("Entrou no df_convenio")

    df_dim = pd.read_sql(query_convenio, connect_rhp())

    df_stage = pd.read_sql(query_convenio_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_CONVENIO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.CONVENIO (CD_CONVENIO, NM_CONVENIO) VALUES (:1, :2)"

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

    print("Dados CONVENIO inseridos")

    df_upd = df_dim.merge(df_stage["CD_CONVENIO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.CONVENIO '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_CONVENIO', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_cor_referencia():
    print("Entrou no df_cor_referencia")

    df_dim = pd.read_sql(query_cor_referencia, connect_rhp())

    df_stage = pd.read_sql(query_cor_referencia_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_COR_REFERENCIA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.SACR_COR_REFERENCIA (CD_COR_REFERENCIA, NM_COR) VALUES (:1, :2)"

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

    print("Dados SACR_COR_REFERENCIA inseridos")

    df_upd = df_dim.merge(df_stage["CD_COR_REFERENCIA"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.SACR_COR_REFERENCIA '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_COR_REFERENCIA', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_diagnostico_atendime(atendimentos):
    print("Entrou no df_diagnostico_atendime")

    df_dim = pd.read_sql(query_diagnostico_atendime.format(atendimentos=atendimentos), connect_rhp())

    df_stage = pd.read_sql(query_diagnostico_atendime_hdata.format(atendimentos=atendimentos), connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_DIAGNOSTICO_ATENDIME"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.DIAGNOSTICO_ATENDIME (CD_CID, CD_DIAGNOSTICO_ATENDIME, CD_ATENDIMENTO) VALUES (:1, :2, :3)"

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

    print("Dados DIAGNOSTICO_ATENDIME inseridos")

    df_upd = df_dim.merge(df_stage["CD_DIAGNOSTICO_ATENDIME"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.DIAGNOSTICO_ATENDIME '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_DIAGNOSTICO_ATENDIME', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_documento_clinico():
    print("Entrou no df_documento_clinico")
    print(dt_back.strftime('%d/%m/%Y'), ' a ', dt.strftime('%d/%m/%Y'))

    df_dim = pd.read_sql(query_documento_clinico.format(data_ini=dt_back.strftime('%d/%m/%Y'), data_fim=dt.strftime('%d/%m/%Y')), connect_rhp())

    df_dim["CD_OBJETO"] = df_dim["CD_OBJETO"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_TIPO_DOCUMENTO"] = df_dim["CD_TIPO_DOCUMENTO"].fillna(0)
    df_dim["TP_STATUS"] = df_dim["TP_STATUS"].fillna("0")

    df_stage = pd.read_sql(query_documento_clinico_hdata.format(data_ini='01/12/2021', data_fim='31/01/2022'), connect_rhp_hdata())

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

    df_upd = df_dim.merge(df_stage["CD_OBJETO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.PW_DOCUMENTO_CLINICO '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_OBJETO', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_esp_med():
    print("Entrou no df_esp_med")

    df_dim = pd.read_sql(query_esp_med, connect_rhp())

    print(df_dim)

    df_stage = pd.read_sql(query_esp_med_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_ESPECIALID"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ESP_MED (CD_ESPECIALID, CD_PRESTADOR, SN_ESPECIAL_PRINCIPAL) VALUES (:1, :2, :3)"

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

    print("Dados ESP_MED inseridos")

    df_upd = df_dim.merge(df_stage["CD_ESPECIALID"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.ESP_MED '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_ESPECIALID', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_especialidad():
    print("Entrou no df_especialidad")

    df_dim = pd.read_sql(query_especialidad, connect_rhp())

    df_stage = pd.read_sql(query_especialidad_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_ESPECIALID"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ESPECIALID (CD_ESPECIALID, DS_ESPECIALID) VALUES (:1, :2)"

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

    print("Dados ESPECIALID inseridos")

    df_upd = df_dim.merge(df_stage["CD_ESPECIALID"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.ESPECIALID '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_ESPECIALID', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_gru_cid():
    print("Entrou no df_gru_cid")

    df_dim = pd.read_sql(query_gru_cid, connect_rhp())

    df_stage = pd.read_sql(query_gru_cid_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_GRU_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.GRU_CID (CD_GRU_CID, DS_GRU_CID) VALUES (:1, :2)"

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

    print("Dados GRU_CID inseridos")

    df_upd = df_dim.merge(df_stage["CD_GRU_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.GRU_CID '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_GRU_CID', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_mot_alt():
    print("Entrou no df_mot_alt")

    df_dim = pd.read_sql(query_mot_alt, connect_rhp())

    df_stage = pd.read_sql(query_mot_alt_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_MOT_ALT"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.MOT_ALT (CD_MOT_ALT, DS_MOT_ALT, TP_MOT_ALTA) VALUES (:1, :2, :3)"

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

    print("Dados MOT_ALT inseridos")

def df_multi_empresa():
    print("Entrou no df_multi_empresa")

    df_dim = pd.read_sql(query_multi_empresa, connect_rhp())

    df_stage = pd.read_sql(query_multi_empresa_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_MULTI_EMPRESA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.MULTI_EMPRESAS (CD_MULTI_EMPRESA, DS_MULTI_EMPRESA) VALUES (:1, :2)"

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

    print("Dados MULTI_EMPRESAS inseridos")

    df_upd = df_dim.merge(df_stage["CD_MULTI_EMPRESA"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.MULTI_EMPRESAS '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_MULTI_EMPRESA', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_ori_ate():
    print("Entrou no df_ori_ate")

    df_dim = pd.read_sql(query_ori_ate, connect_rhp())

    df_stage = pd.read_sql(query_ori_ate_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_ORI_ATE"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ORI_ATE (CD_ORI_ATE, DS_ORI_ATE, TP_ORIGEM, CD_SETOR) VALUES (:1, :2, :3, :4)"

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

    print("Dados ORI_ATE inseridos")

    df_upd = df_dim.merge(df_stage["CD_ORI_ATE"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.ORI_ATE '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_ORI_ATE', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_prestador():
    print("Entrou no df_prestador")

    df_dim = pd.read_sql(query_prestador, connect_rhp())

    print(df_dim['DT_NASCIMENTO'][df_dim['DT_NASCIMENTO'] > dt])

    df_dim['DT_NASCIMENTO'][df_dim['DT_NASCIMENTO'] > dt] = None

    print(df_dim['DT_NASCIMENTO'][df_dim['DT_NASCIMENTO'] > dt])

    df_stage = pd.read_sql(query_prestador_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_PRESTADOR"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.PRESTADOR (CD_PRESTADOR, NM_PRESTADOR, DT_NASCIMENTO, TP_PRESTADOR, CD_TIP_PRESTA) VALUES (:1, :2, :3, :4, :5)"

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

    print("Dados PRESTADOR inseridos")

    df_upd = df_dim.merge(df_stage["CD_PRESTADOR"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.PRESTADOR '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_PRESTADOR', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_paciente():
    print("Entrou no df_paciente")

    df_dim = pd.read_sql(query_paciente, connect_rhp())

    print(df_dim.info())

    df_stage = pd.read_sql(query_paciente_hdata, connect_rhp_hdata())
    df_stage["DT_NASCIMENTO"] = df_stage["DT_NASCIMENTO"].astype(str)
    print(df_stage.info())

    df_diff = df_dim.merge(df_stage["CD_PACIENTE"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print(df_diff['DT_NASCIMENTO'])

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.PACIENTE (CD_PACIENTE, DT_NASCIMENTO, TP_SEXO, DT_CADASTRO, NM_BAIRRO) VALUES (:1, :2, :3, :4, :5)"

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

    print("Dados PACIENTE inseridos")

    df_upd = df_dim.merge(df_stage["CD_PACIENTE"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.PACIENTE '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_PACIENTE', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_pagu_objeto():
    print("Entrou no df_pagu_objeto")

    df_dim = pd.read_sql(query_pagu_objeto, connect_rhp())

    df_stage = pd.read_sql(query_pagu_objeto_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_OBJETO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.PAGU_OBJETO (CD_OBJETO, TP_OBJETO) VALUES (:1, :2)"

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

    print("Dados PAGU_OBJETO inseridos")

    df_upd = df_dim.merge(df_stage["CD_OBJETO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.PAGU_OBJETO '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_OBJETO', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_registro_alta():
    print("Entrou no df_registro_alta")
    print(dt_back.strftime('%d/%m/%Y'), ' a ', dt.strftime('%d/%m/%Y'))

    df_dim = pd.read_sql(query_registro_alta.format(data_ini=dt_back.strftime('%d/%m/%Y'), data_fim=dt.strftime('%d/%m/%Y')), connect_rhp())
    
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)

    df_stage = pd.read_sql(query_registro_alta_hdata.format(data_ini='01/10/2021', data_fim='31/01/2022'), connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.PW_REGISTRO_ALTA (HR_ALTA_MEDICA, CD_ATENDIMENTO) VALUES (:1, :2)"

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

    print("Dados PW_REGISTRO_ALTA inseridos")

    df_upd = df_dim.merge(df_stage["CD_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.PW_REGISTRO_ALTA '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_ATENDIMENTO', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_setor():
    print("Entrou no df_setor")

    df_dim = pd.read_sql(query_setor, connect_rhp())

    df_stage = pd.read_sql(query_setor_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_SETOR"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.SETOR (CD_SETOR, NM_SETOR) VALUES (:1, :2)"

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

    print("Dados SETOR inseridos")

    df_upd = df_dim.merge(df_stage["CD_SETOR"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.SETOR '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_SETOR', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_sgru_cid():
    print("Entrou no df_sgru_cid")

    df_dim = pd.read_sql(query_sgru_cid, connect_rhp())

    df_stage = pd.read_sql(query_sgru_cid_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_SGRU_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.SGRU_CID (CD_SGRU_CID, CD_GRU_CID, DS_SGRU_CID) VALUES (:1, :2, :3)"

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

    print("Dados SGRU_CID inseridos")

    df_upd = df_dim.merge(df_stage["CD_SGRU_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.SGRU_CID '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_SGRU_CID', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_sintoma_avaliacao():
    print("Entrou no df_sintoma_avaliacao")

    df_dim = pd.read_sql(query_sintoma_avaliacao, connect_rhp())

    df_stage = pd.read_sql(query_sintoma_avaliacao_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_SINTOMA_AVALIACAO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.SACR_SINTOMA_AVALIACAO (CD_SINTOMA_AVALIACAO, DS_SINTOMA) VALUES (:1, :2)"

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

    print("Dados SACR_SINTOMA_AVALIACAO inseridos")

    df_upd = df_dim.merge(df_stage["CD_SINTOMA_AVALIACAO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.SACR_SINTOMA_AVALIACAO '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_SINTOMA_AVALIACAO', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_tempo_processo():
    print("Entrou no df_tempo_processo")
    print(dt_back.strftime('%d/%m/%Y'), ' a ', dt.strftime('%d/%m/%Y'))

    df_dim = pd.read_sql(query_tempo_processo.format(data_ini=dt_back.strftime('%d/%m/%Y'), data_fim=dt.strftime('%d/%m/%Y')), connect_rhp())

    df_dim["CD_TIPO_TEMPO_PROCESSO"] = df_dim["CD_TIPO_TEMPO_PROCESSO"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)

    df_stage = pd.read_sql(query_tempo_processo_hdata.format(data_ini="01/06/2021", data_fim="31/01/2022"), connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.SACR_TEMPO_PROCESSO (DH_PROCESSO, CD_TIPO_TEMPO_PROCESSO, CD_ATENDIMENTO) VALUES (:1, :2, :3)"

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

    df_upd = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.SACR_TEMPO_PROCESSO '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_tip_mar():
    print("Entrou no df_tip_mar")

    df_dim = pd.read_sql(query_tip_mar, connect_rhp())

    df_stage = pd.read_sql(query_tip_mar_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_TIP_MAR"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.TIP_MAR (CD_TIP_MAR) VALUES (:1)"

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

    print("Dados TIP_MAR inseridos")

    df_upd = df_dim.merge(df_stage["CD_TIP_MAR"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.TIP_MAR '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_TIP_MAR', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_tip_res():
    print("Entrou no df_tip_res")

    df_dim = pd.read_sql(query_tip_res, connect_rhp())

    df_stage = pd.read_sql(query_tip_res_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_TIP_RES"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)
    
    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.TIP_RES (CD_TIP_RES, DS_TIP_RES, SN_OBITO) VALUES (:1, :2, :3)"

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

    print("Dados TIP_RES inseridos")

    df_upd = df_dim.merge(df_stage["CD_TIP_RES"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.TIP_RES '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_TIP_RES', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_triagem_atendimento():
    print("Entrou no df_triagem_atendimento")
    print(dt_back.strftime('%d/%m/%Y'), ' a ', dt.strftime('%d/%m/%Y'))

    df_dim = pd.read_sql(query_triagem_atendimento.format(data_ini=dt_back.strftime('%d/%m/%Y'), data_fim=dt.strftime('%d/%m/%Y')), connect_rhp())

    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_TRIAGEM_ATENDIMENTO"] = df_dim["CD_TRIAGEM_ATENDIMENTO"].fillna(0)
    df_dim["CD_SINTOMA_AVALIACAO"] = df_dim["CD_SINTOMA_AVALIACAO"].fillna(0)
    df_dim["DS_SENHA"] = df_dim["DS_SENHA"].fillna("0")

    df_stage = pd.read_sql(query_triagem_atendimento_hdata.format(data_ini=dt_back.strftime('%d/%m/%Y'), data_fim=dt.strftime('%d/%m/%Y')), connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_TRIAGEM_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)
    
    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.TRIAGEM_ATENDIMENTO (CD_ATENDIMENTO, CD_TRIAGEM_ATENDIMENTO, CD_SINTOMA_AVALIACAO, DS_SENHA, DH_PRE_ATENDIMENTO) VALUES (:1, :2, :3, :4, :5)"

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

    print("Dados TRIAGEM_ATENDIMENTO inseridos")

    df_upd = df_dim.merge(df_stage["CD_TRIAGEM_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.TRIAGEM_ATENDIMENTO '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_TRIAGEM_ATENDIMENTO', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

def df_usuario():
    print("Entrou no df_usuario")

    df_dim = pd.read_sql(query_usuario, connect_rhp())

    df_stage = pd.read_sql(query_usuario_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_USUARIO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)
    
    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.USUARIOS (CD_USUARIO, NM_USUARIO) VALUES (:1, :2)"

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

    print("Dados USUARIOS inseridos")

    df_upd = df_dim.merge(df_stage["CD_USUARIO"],indicator = True, how='left').loc[lambda x : x['_merge'] =='both']
    df_upd = df_upd.drop(columns=['_merge'])
    df_upd = df_upd.reset_index(drop=True)

    print("dados para update")
    print(df_upd.info())

    d = df_upd.to_dict(orient='split')
    print(d)
    for i in range(len(d['columns']) - 1):
        conn = connect_rhp_hdata()
        cursor = conn.cursor()

        query = ''
        query = 'UPDATE MV_RHP.USUARIOS '
        query += 'SET {nome_coluna} = CASE {cd} '.format(nome_coluna=d['columns'][i + 1],
                                                          cd=d['columns'][0])
        todos_cds = ''
        for j in d['data']:
            if pd.isna(j[i + 1]):
                query += 'WHEN {cd_p_update} THEN null  '.format(cd_p_update=j[0])
            elif 'cd' in d['columns'][i + 1] and 'dt' not in d['columns'][i + 1] and 'cid' not in d['columns'][i + 1]:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=int(j[i + 1]))
            else:
                if type(d['columns'][i + 1]) == np.int64 or type(d['columns'][i + 1]) == np.float64:
                    query += 'WHEN {cd_p_update} THEN {novo_valor} '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
                else:
                    query += 'WHEN {cd_p_update} THEN \'{novo_valor}\' '.format(cd_p_update=j[0],
                                                                             novo_valor=j[i + 1])
            todos_cds += "'" + str(j[0]) + "'" + ','
        todos_cds = todos_cds[:-1]
        query += 'ELSE {nome_coluna} '.format(nome_coluna=d['columns'][i + 1])
        query += 'END '
        query += 'WHERE {cd} IN({todos_cds})'.format(cd='CD_USUARIO', todos_cds=todos_cds)

        print(query)
        # cursor.execute(query)
        conn.commit()
        conn.close()

dt = datetime.datetime.today()
dt_back = dt - datetime.timedelta(days=5)

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

t5 = PythonOperator(
    task_id="insert_cor_referencia_rhp",
    python_callable=df_cor_referencia,
    dag=dag)

t7 = PythonOperator(
    task_id="insert_documento_clinico_rhp",
    python_callable=df_documento_clinico,
    dag=dag)

t8 = PythonOperator(
    task_id="insert_esp_med_rhp",
    python_callable=df_esp_med,
    dag=dag)

t9 = PythonOperator(
    task_id="insert_especialidad_rhp",
    python_callable=df_especialidad,
    dag=dag)

t10 = PythonOperator(
    task_id="insert_gru_cid_rhp",
    python_callable=df_gru_cid,
    dag=dag)

t10 = PythonOperator(
    task_id="insert_prestador_rhp",
    python_callable=df_prestador,
    dag=dag)

# t11 = PythonOperator(
#     task_id="insert_mot_alt_rhp",
#     python_callable=df_mot_alt,
#     dag=dag)

t12 = PythonOperator(
    task_id="insert_multi_empresa_rhp",
    python_callable=df_multi_empresa,
    dag=dag)

t13 = PythonOperator(
    task_id="insert_ori_ate_rhp",
    python_callable=df_ori_ate,
    dag=dag)

t14 = PythonOperator(
    task_id="insert_paciente_rhp",
    python_callable=df_paciente,
    dag=dag)

t15 = PythonOperator(
    task_id="insert_pagu_objeto_rhp",
    python_callable=df_pagu_objeto,
    dag=dag)

t16 = PythonOperator(
    task_id="insert_registro_alta_rhp",
    python_callable=df_registro_alta,
    dag=dag)

t17 = PythonOperator(
    task_id="insert_setor_rhp",
    python_callable=df_setor,
    dag=dag)

t18 = PythonOperator(
    task_id="insert_sgru_cid_rhp",
    python_callable=df_sgru_cid,
    dag=dag)

t19 = PythonOperator(
    task_id="insert_sintoma_avaliacao_rhp",
    python_callable=df_sintoma_avaliacao,
    dag=dag)

t20 = PythonOperator(
    task_id="insert_tempo_processo_rhp",
    python_callable=df_tempo_processo,
    dag=dag)

t21 = PythonOperator(
    task_id="insert_tip_mar_rhp",
    python_callable=df_tip_mar,
    dag=dag)

t22 = PythonOperator(
    task_id="insert_tip_res_rhp",
    python_callable=df_tip_res,
    dag=dag)

t23 = PythonOperator(
    task_id="insert_triagem_atendimento_rhp",
    python_callable=df_triagem_atendimento,
    dag=dag)

t24 = PythonOperator(
    task_id="insert_usuario_rhp",
    python_callable=df_usuario,
    dag=dag)

(t1, t3, t4, t5, t8, t9, t10, t12, t13, t14, t15, t17, t18, t19, t21, t22, t24) >> t16 >> t23 >> t20 >> t7 >> t2 >> t0