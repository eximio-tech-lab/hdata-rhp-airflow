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
    print(dt.strftime('%d/%m/%Y'))

    df_dim = pd.read_sql(query_atendime.format(data_ini=dt.strftime('%d/%m/%Y')), connect_rhp())

    # df_dim["DT_ATENDIMENTO"] = df_dim["DT_ATENDIMENTO"].fillna("01.01.1899 00:00:00")
    # df_dim["HR_ATENDIMENTO"] = df_dim["HR_ATENDIMENTO"].fillna("01.01.1899 00:00:00")
    # df_dim["HR_ALTA"] = df_dim["HR_ALTA"].fillna("01.01.1899 00:00:00")
    # df_dim["HR_ALTA_MEDICA"] = df_dim["HR_ALTA_MEDICA"].fillna("01.01.1899 00:00:00")
    # df_dim["CD_MULTI_EMPRESA"] = df_dim["CD_MULTI_EMPRESA"].fillna(0)
    # df_dim["CD_PACIENTE"] = df_dim["CD_PACIENTE"].fillna(0)
    # df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    # df_dim["CD_CID"] = df_dim["CD_CID"].fillna("0")
    # df_dim["CD_MOT_ALT"] = df_dim["CD_MOT_ALT"].fillna(0)
    # df_dim["CD_TIP_RES"] = df_dim["CD_TIP_RES"].fillna(0)
    # df_dim["CD_CONVENIO"] = df_dim["CD_CONVENIO"].fillna(0)
    # df_dim["CD_ESPECIALID"] = df_dim["CD_ESPECIALID"].fillna(0)
    # df_dim["CD_PRESTADOR"] = df_dim["CD_PRESTADOR"].fillna(0)
    # df_dim["CD_ATENDIMENTO_PAI"] = df_dim["CD_ATENDIMENTO_PAI"].fillna(0)
    # df_dim["CD_LEITO"] = df_dim["CD_LEITO"].fillna(0)
    # df_dim["CD_ORI_ATE"] = df_dim["CD_ORI_ATE"].fillna(0)
    # df_dim["CD_SERVICO"] = df_dim["CD_SERVICO"].fillna(0)
    # df_dim["TP_ATENDIMENTO"] = df_dim["TP_ATENDIMENTO"].fillna("0")
    # df_dim["CD_TIP_MAR"] = df_dim["CD_TIP_MAR"].fillna(0)
    # df_dim["CD_SINTOMA_AVALIACAO"] = df_dim["CD_SINTOMA_AVALIACAO"].fillna(0)
    # df_dim["NM_USUARIO_ALTA_MEDICA"] = df_dim["NM_USUARIO_ALTA_MEDICA"].fillna("0")

    df_dim['HR_ALTA'] = df_dim['HR_ALTA'].astype(str)
    df_dim['HR_ALTA_MEDICA'] = df_dim['HR_ALTA_MEDICA'].astype(str)

    lista_cds_atendimentos = df_dim['CD_ATENDIMENTO'].to_list()
    lista_cds_atendimentos = [str(cd) for cd in lista_cds_atendimentos]
    atendimentos = ','.join(lista_cds_atendimentos)

    print(df_dim.info())

    df_stage = pd.read_sql(query_atendime_hdata.format(data_ini=dt.strftime('%d/%m/%Y')), connect_rhp_hdata())

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

    df_diagnostico_atendime(atendimentos)

def df_cid():
    print("Entrou no df_cid")

    df_dim = pd.read_sql(query_cid, connect_rhp())

    # df_dim["CD_CID"] = df_dim["CD_CID"].fillna("0")
    # df_dim["DS_CID"] = df_dim["DS_CID"].fillna("0")
    # df_dim["CD_SGRU_CID"] = df_dim["CD_SGRU_CID"].fillna("0")

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

def df_classificacao_risco():
    print("Entrou no df_classificacao_risco")
    print(dt.strftime('%d/%m/%Y'))

    df_dim = pd.read_sql(query_classificacao_risco.format(data_ini=dt.strftime('%d/%m/%Y')), connect_rhp())

    print(df_dim.info())

    # df_dim["CD_CLASSIFICACAO_RISCO"] = df_dim["CD_CLASSIFICACAO_RISCO"].fillna(0)
    # df_dim["CD_COR_REFERENCIA"] = df_dim["CD_COR_REFERENCIA"].fillna(0)
    # df_dim["CD_TRIAGEM_ATENDIMENTO"] = df_dim["CD_TRIAGEM_ATENDIMENTO"].fillna(0)
    # df_dim["DH_CLASSIFICACAO_RISCO"] = df_dim["DH_CLASSIFICACAO_RISCO"].fillna("01.01.1899 00:00:00")

    df_stage = pd.read_sql(query_classificacao_risco_hdata.format(data_ini=dt.strftime('%d/%m/%Y')), connect_rhp_hdata())
    print(df_stage.info())

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

def df_classificacao():
    print("Entrou no df_classificacao")

    df_dim = pd.read_sql(query_classificacao, connect_rhp())

    # df_dim["CD_CLASSIFICACAO"] = df_dim["CD_CLASSIFICACAO"].fillna(0)
    # df_dim["DS_TIPO_RISCO"] = df_dim["DS_TIPO_RISCO"].fillna("0")

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

def df_convenio():
    print("Entrou no df_convenio")

    df_dim = pd.read_sql(query_convenio, connect_rhp())

    # df_dim["CD_CONVENIO"] = df_dim["CD_CONVENIO"].fillna(0)
    # df_dim["NM_CONVENIO"] = df_dim["NM_CONVENIO"].fillna("0")

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

def df_cor_referencia():
    print("Entrou no df_cor_referencia")

    df_dim = pd.read_sql(query_cor_referencia, connect_rhp())

    # df_dim["CD_COR_REFERENCIA"] = df_dim["CD_COR_REFERENCIA"].fillna(0)

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

def df_diagnostico_atendime(atendimentos):
    print("Entrou no df_diagnostico_atendime")

    df_dim = pd.read_sql(query_diagnostico_atendime.format(atendimentos=atendimentos), connect_rhp())

    # df_dim["CD_CID"] = df_dim["CD_CID"].fillna("0")
    # df_dim["CD_DIAGNOSTICO_ATENDIME"] = df_dim["CD_DIAGNOSTICO_ATENDIME"].fillna(0)
    # df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)

    df_stage = pd.read_sql(query_diagnostico_atendime_hdata.format(atendimentos=atendimentos), connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
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

def df_documento_clinico():
    print("Entrou no df_documento_clinico")
    print(dt.strftime('%d/%m/%Y'))

    df_dim = pd.read_sql(query_documento_clinico.format(data_ini=dt.strftime('%d/%m/%Y')), connect_rhp())

    # df_dim["CD_OBJETO"] = df_dim["CD_OBJETO"].fillna(0)
    # df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    # df_dim["CD_TIPO_DOCUMENTO"] = df_dim["CD_TIPO_DOCUMENTO"].fillna(0)
    # df_dim["TP_STATUS"] = df_dim["TP_STATUS"].fillna("0")
    # df_dim["DH_CRIACAO"] = df_dim["DH_CRIACAO"].fillna("01.01.1899 00:00:00")

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

def df_esp_med():
    print("Entrou no df_esp_med")

    df_dim = pd.read_sql(query_esp_med, connect_rhp())

    print(df_dim)

    # df_dim["CD_ESPECIALID"] = df_dim["CD_ESPECIALID"].fillna(0)
    # df_dim["CD_PRESTADOR"] = df_dim["CD_PRESTADOR"].fillna(0)
    # df_dim["SN_ESPECIAL_PRINCIPAL"] = df_dim["SN_ESPECIAL_PRINCIPAL"].fillna("N")

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

def df_especialidad():
    print("Entrou no df_especialidad")

    df_dim = pd.read_sql(query_especialidad, connect_rhp())

    # df_dim["CD_ESPECIALID"] = df_dim["CD_ESPECIALID"].fillna(0)
    # df_dim["DS_ESPECIALID"] = df_dim["DS_ESPECIALID"].fillna("0")

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

def df_gru_cid():
    print("Entrou no df_gru_cid")

    df_dim = pd.read_sql(query_gru_cid, connect_rhp())

    # df_dim["CD_GRU_CID"] = df_dim["CD_GRU_CID"].fillna(0)
    # df_dim["DS_GRU_CID"] = df_dim["DS_GRU_CID"].fillna("0")

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

def df_mot_alt():
    print("Entrou no df_mot_alt")

    df_dim = pd.read_sql(query_mot_alt, connect_rhp())

    # df_dim["CD_MOT_ALT"] = df_dim["CD_MOT_ALT"].fillna(0)
    # df_dim["DS_MOT_ALT"] = df_dim["DS_MOT_ALT"].fillna("0")
    # df_dim["TP_MOT_ALTA"] = df_dim["TP_MOT_ALTA"].fillna("0")

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

    # df_dim["CD_MULTI_EMPRESA"] = df_dim["CD_MULTI_EMPRESA"].fillna(0)
    # df_dim["DS_MULTI_EMPRESA"] = df_dim["DS_MULTI_EMPRESA"].fillna("0")

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

def df_ori_ate():
    print("Entrou no df_ori_ate")

    df_dim = pd.read_sql(query_ori_ate, connect_rhp())

    # df_dim["CD_ORI_ATE"] = df_dim["CD_ORI_ATE"].fillna(0)
    # df_dim["DS_ORI_ATE"] = df_dim["DS_ORI_ATE"].fillna("0")
    # df_dim["TP_ORIGEM"] = df_dim["TP_ORIGEM"].fillna("0")
    # df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)

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

def df_prestador():
    print("Entrou no df_prestador")

    df_dim = pd.read_sql(query_prestador, connect_rhp())

    # df_dim["CD_PRESTADOR"] = df_dim["CD_PRESTADOR"].fillna(0)
    # df_dim["NM_PRESTADOR"] = df_dim["NM_PRESTADOR"].fillna("0")
    # df_dim["DT_NASCIMENTO"] = df_dim["DT_NASCIMENTO"].fillna("01.01.1899 00:00:00")
    # df_dim["TP_PRESTADOR"] = df_dim["TP_PRESTADOR"].fillna("0")
    # df_dim["CD_TIP_PRESTA"] = df_dim["CD_TIP_PRESTA"].fillna(0)

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

def df_paciente():
    print("Entrou no df_paciente")

    df_dim = pd.read_sql(query_paciente, connect_rhp())

    # df_dim["CD_PACIENTE"] = df_dim["CD_PACIENTE"].fillna(0)
    # df_dim["DT_NASCIMENTO"] = pd.to_datetime(df_dim["DT_NASCIMENTO"])
    # df_dim["TP_SEXO"] = df_dim["TP_SEXO"].fillna("0")
    # df_dim["DT_CADASTRO"] = df_dim["DT_CADASTRO"].fillna("01.01.1899 00:00:00")
    # df_dim["NM_BAIRRO"] = df_dim["NM_BAIRRO"].fillna("0")

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

def df_pagu_objeto():
    print("Entrou no df_pagu_objeto")

    df_dim = pd.read_sql(query_pagu_objeto, connect_rhp())

    # df_dim["CD_OBJETO"] = df_dim["CD_OBJETO"].fillna(0)
    # df_dim["TP_OBJETO"] = df_dim["TP_OBJETO"].fillna("0")

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

def df_registro_alta():
    print("Entrou no df_registro_alta")
    print(dt.strftime('%d/%m/%Y'))

    df_dim = pd.read_sql(query_registro_alta.format(data_ini=dt.strftime('%d/%m/%Y')), connect_rhp())

    # df_dim["HR_ALTA_MEDICA"] = df_dim["HR_ALTA_MEDICA"].fillna("1899-01-01 00:00:00")
    # df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)

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

def df_setor():
    print("Entrou no df_setor")

    df_dim = pd.read_sql(query_setor, connect_rhp())

    # df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)
    # df_dim["NM_SETOR"] = df_dim["NM_SETOR"].fillna("0")

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

def df_sgru_cid():
    print("Entrou no df_sgru_cid")

    df_dim = pd.read_sql(query_sgru_cid, connect_rhp())

    # df_dim["CD_SGRU_CID"] = df_dim["CD_SGRU_CID"].fillna("0")
    # df_dim["CD_GRU_CID"] = df_dim["CD_GRU_CID"].fillna(0)
    # df_dim["DS_SGRU_CID"] = df_dim["DS_SGRU_CID"].fillna("0")

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

def df_sintoma_avaliacao():
    print("Entrou no df_sintoma_avaliacao")

    df_dim = pd.read_sql(query_sintoma_avaliacao, connect_rhp())

    # df_dim["CD_SINTOMA_AVALIACAO"] = df_dim["CD_SINTOMA_AVALIACAO"].fillna(0)
    # df_dim["DS_SINTOMA"] = df_dim["DS_SINTOMA"].fillna("0")

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

def df_tempo_processo():
    print("Entrou no df_tempo_processo")
    print(dt.strftime('%d/%m/%Y'))

    df_dim = pd.read_sql(query_tempo_processo.format(data_ini=dt.strftime('%d/%m/%Y')), connect_rhp())

    # df_dim["DH_PROCESSO"] = df_dim["DH_PROCESSO"].fillna("01.01.1899 00:00:00")
    # df_dim["CD_TIPO_TEMPO_PROCESSO"] = df_dim["CD_TIPO_TEMPO_PROCESSO"].fillna(0)
    # df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)

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

def df_tip_mar():
    print("Entrou no df_tip_mar")

    df_dim = pd.read_sql(query_tip_mar, connect_rhp())

    # df_dim["CD_TIP_MAR"] = df_dim["CD_TIP_MAR"].fillna(0)

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

def df_tip_res():
    print("Entrou no df_tip_res")

    df_dim = pd.read_sql(query_tip_res, connect_rhp())

    # df_dim["CD_TIP_RES"] = df_dim["CD_TIP_RES"].fillna(0)
    # df_dim["DS_TIP_RES"] = df_dim["DS_TIP_RES"].fillna("0")
    # df_dim["SN_OBITO"] = df_dim["SN_OBITO"].fillna("0")

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

def df_triagem_atendimento():
    print("Entrou no df_triagem_atendimento")
    print(dt.strftime('%d/%m/%Y'))

    df_dim = pd.read_sql(query_triagem_atendimento.format(data_ini=dt.strftime('%d/%m/%Y')), connect_rhp())

    # df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    # df_dim["CD_TRIAGEM_ATENDIMENTO"] = df_dim["CD_TRIAGEM_ATENDIMENTO"].fillna(0)
    # df_dim["CD_SINTOMA_AVALIACAO"] = df_dim["CD_SINTOMA_AVALIACAO"].fillna(0)
    # df_dim["DS_SENHA"] = df_dim["DS_SENHA"].fillna("0")
    # df_dim["DH_PRE_ATENDIMENTO"] = df_dim["DH_PRE_ATENDIMENTO"].fillna("01.01.1899 00:00:00")

    df_stage = pd.read_sql(query_triagem_atendimento_hdata.format(data_ini=dt.strftime('%d/%m/%Y')), connect_rhp_hdata())

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

def df_usuario():
    print("Entrou no df_usuario")

    df_dim = pd.read_sql(query_usuario, connect_rhp())

    # df_dim["CD_USUARIO"] = df_dim["CD_USUARIO"].fillna("0")
    # df_dim["NM_USUARIO"] = df_dim["NM_USUARIO"].fillna("0")

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

def df_pre_med():
    print("Entrou no df_pre_med")

    df_dim = pd.read_sql(query_pre_med.format(data_ini='2021-01-01 00:00:00', data_fim='2021-12-31 23:59:59'), connect_rhp())

    df_dim["CD_PRE_MED"] = df_dim["CD_PRE_MED"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_PRESTADOR"] = df_dim["CD_PRESTADOR"].fillna(0)
    df_dim["CD_DOCUMENTO_CLINICO"] = df_dim["CD_DOCUMENTO_CLINICO"].fillna(0)
    # df_dim["DT_PRE_MED"] = df_dim["DT_PRE_MED"].fillna("01.01.1899 00:00:00")
    df_dim["TP_PRE_MED"] = df_dim["TP_PRE_MED"].fillna("0")
    df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)

    df_stage = pd.read_sql(query_pre_med_hdata.format(data_ini='2021-01-01 00:00:00', data_fim='2021-12-31 23:59:59'), connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)
    
    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.PRE_MED (CD_PRE_MED, CD_ATENDIMENTO, CD_PRESTADOR, CD_DOCUMENTO_CLINICO, DT_PRE_MED, TP_PRE_MED, CD_SETOR) VALUES (:1, :2, :3, :4, :5, :6, :7)"

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

    print("Dados PRE_MED inseridos")

    df_dim = pd.read_sql(query_pre_med.format(data_ini='2020-01-01 00:00:00', data_fim='2020-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_PRE_MED"] = df_dim["CD_PRE_MED"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_PRESTADOR"] = df_dim["CD_PRESTADOR"].fillna(0)
    df_dim["CD_DOCUMENTO_CLINICO"] = df_dim["CD_DOCUMENTO_CLINICO"].fillna(0)
    # df_dim["DT_PRE_MED"] = df_dim["DT_PRE_MED"].fillna("01.01.1899 00:00:00")
    df_dim["TP_PRE_MED"] = df_dim["TP_PRE_MED"].fillna("0")
    df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)

    df_stage = pd.read_sql(query_pre_med_hdata.format(data_ini='2020-01-01 00:00:00', data_fim='2020-12-31 23:59:59'), connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)
    
    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.PRE_MED (CD_PRE_MED, CD_ATENDIMENTO, CD_PRESTADOR, CD_DOCUMENTO_CLINICO, DT_PRE_MED, TP_PRE_MED, CD_SETOR) VALUES (:1, :2, :3, :4, :5, :6, :7)"

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

    print("Dados PRE_MED inseridos")

    df_dim = pd.read_sql(query_pre_med.format(data_ini='2019-01-01 00:00:00', data_fim='2019-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_PRE_MED"] = df_dim["CD_PRE_MED"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_PRESTADOR"] = df_dim["CD_PRESTADOR"].fillna(0)
    df_dim["CD_DOCUMENTO_CLINICO"] = df_dim["CD_DOCUMENTO_CLINICO"].fillna(0)
    # df_dim["DT_PRE_MED"] = df_dim["DT_PRE_MED"].fillna("01.01.1899 00:00:00")
    df_dim["TP_PRE_MED"] = df_dim["TP_PRE_MED"].fillna("0")
    df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)

    df_stage = pd.read_sql(query_pre_med_hdata.format(data_ini='2019-01-01 00:00:00', data_fim='2019-12-31 23:59:59'), connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)
    
    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.PRE_MED (CD_PRE_MED, CD_ATENDIMENTO, CD_PRESTADOR, CD_DOCUMENTO_CLINICO, DT_PRE_MED, TP_PRE_MED, CD_SETOR) VALUES (:1, :2, :3, :4, :5, :6, :7)"

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

    print("Dados PRE_MED inseridos")

def df_itpre_med():
    print("Entrou no df_itpre_med")

    df_dim = pd.read_sql(query_itpre_med, connect_rhp())

    print(df_dim)

    df_dim["CD_PRE_MED"] = df_dim["CD_PRE_MED"].fillna(0)
    df_dim["CD_ITPRE_MED"] = df_dim["CD_ITPRE_MED"].fillna(0)
    df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
    df_dim["CD_TIP_PRESC"] = df_dim["CD_TIP_PRESC"].fillna(0)
    df_dim["CD_TIP_ESQ"] = df_dim["CD_TIP_ESQ"].fillna("0")
    df_dim["CD_FOR_APL"] = df_dim["CD_FOR_APL"].fillna("0")
    df_dim["CD_TIP_FRE"] = df_dim["CD_TIP_FRE"].fillna(0)
    df_dim["TP_JUSTIFICATIVA"] = df_dim["TP_JUSTIFICATIVA"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ITPRE_MED (CD_PRE_MED, CD_ITPRE_MED, CD_PRODUTO, CD_TIP_PRESC, CD_TIP_ESQ, CD_FOR_APL, CD_TIP_FRE, TP_JUSTIFICATIVA) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"

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

    print("Dados ITPRE_MED inseridos")

def df_tip_presc():
    print("Entrou no df_tip_presc")

    df_dim = pd.read_sql(query_tip_presc, connect_rhp())

    print(df_dim)

    # df_dim["CD_TIP_PRESC"] = df_dim["CD_TIP_PRESC"].fillna(0)
    # df_dim["DS_TIP_PRESC"] = df_dim["DS_TIP_PRESC"].fillna("0")
    # df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.TIP_PRESC (CD_TIP_PRESC, DS_TIP_PRESC, CD_PRO_FAT) VALUES (:1, :2, :3)"

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

    print("Dados TIP_PRESC inseridos")

def df_for_apl():
    print("Entrou no df_for_apl")

    df_dim = pd.read_sql(query_for_apl, connect_rhp())

    print(df_dim)

    df_dim["CD_FOR_APL"] = df_dim["CD_FOR_APL"].fillna(0)
    df_dim["DS_FOR_APL"] = df_dim["DS_FOR_APL"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.FOR_APL (CD_FOR_APL, DS_FOR_APL) VALUES (:1, :2)"

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

    print("Dados FOR_APL inseridos")

def df_tip_esq():
    print("Entrou no df_tip_esq")

    df_dim = pd.read_sql(query_tip_esq, connect_rhp())

    print(df_dim)

    df_dim["CD_TIP_ESQ"] = df_dim["CD_TIP_ESQ"].fillna("0")
    df_dim["DS_TIP_ESQ"] = df_dim["DS_TIP_ESQ"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.TIP_ESQ (CD_TIP_ESQ, DS_TIP_ESQ) VALUES (:1, :2)"

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

    print("Dados TIP_ESQ inseridos")

def df_tip_fre():
    print("Entrou no df_tip_fre")

    df_dim = pd.read_sql(query_tip_fre, connect_rhp())

    print(df_dim)

    df_dim["CD_TIP_FRE"] = df_dim["CD_TIP_FRE"].fillna(0)
    df_dim["DS_TIP_FRE"] = df_dim["DS_TIP_FRE"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.TIP_FRE (CD_TIP_FRE, DS_TIP_FRE) VALUES (:1, :2)"

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

    print("Dados TIP_FRE inseridos")

# def df_de_para_tuss():
#     print("Entrou no df_de_para_tuss")

    # df_dim = pd.read_sql(query_de_para_tuss, connect_rhp())

    # print(df)

def df_gru_fat():
    print("Entrou no df_gru_fat")

    df_dim = pd.read_sql(query_gru_fat, connect_rhp())

    print(df_dim)

    df_dim["CD_GRU_FAT"] = df_dim["CD_GRU_FAT"].fillna(0)
    df_dim["DS_GRU_FAT"] = df_dim["DS_GRU_FAT"].fillna("0")
    df_dim["TP_GRU_FAT"] = df_dim["TP_GRU_FAT"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.GRU_FAT (CD_GRU_FAT, DS_GRU_FAT, TP_GRU_FAT) VALUES (:1, :2, :3)"

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

    print("Dados GRU_FAT inseridos")

def df_gru_pro():
    print("Entrou no df_gru_pro")

    df_dim = pd.read_sql(query_gru_pro, connect_rhp())

    print(df_dim)

    df_dim["CD_GRU_PRO"] = df_dim["CD_GRU_PRO"].fillna(0)
    df_dim["CD_GRU_FAT"] = df_dim["CD_GRU_FAT"].fillna(0)
    df_dim["DS_GRU_PRO"] = df_dim["DS_GRU_PRO"].fillna("0")
    df_dim["TP_GRU_PRO"] = df_dim["TP_GRU_PRO"].fillna("0")
    df_dim["TP_CUSTO"] = df_dim["TP_CUSTO"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.GRU_PRO (CD_GRU_PRO, CD_GRU_FAT, DS_GRU_PRO, TP_GRU_PRO, TP_CUSTO) VALUES (:1, :2, :3, :4, :5)"

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

    print("Dados GRU_PRO inseridos")

def df_produto():
    print("Entrou no df_produto")

    df_dim = pd.read_sql(query_produto, connect_rhp())

    print(df_dim)

    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    df_dim["DS_PRODUTO"] = df_dim["DS_PRODUTO"].fillna("0")
    df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
    df_dim["VL_FATOR_PRO_FAT"] = df_dim["VL_FATOR_PRO_FAT"].fillna(0)
    df_dim["SN_OPME"] = df_dim["SN_OPME"].fillna("0")
    df_dim["CD_ESPECIE"] = df_dim["CD_ESPECIE"].fillna(0)
    df_dim["VL_CUSTO_MEDIO"] = df_dim["VL_CUSTO_MEDIO"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.PRODUTO (CD_PRO_FAT, DS_PRODUTO, CD_PRODUTO, VL_FATOR_PRO_FAT, SN_OPME, CD_ESPECIE, VL_CUSTO_MEDIO) VALUES (:1, :2, :3, :4, :5, :6, :7)"

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

    print("Dados PRODUTO inseridos")

def df_pro_fat():
    print("Entrou no df_pro_fat")

    df_dim = pd.read_sql(query_pro_fat, connect_rhp())

    print(df_dim)

    df_dim["CD_GRU_PRO"] = df_dim["CD_GRU_PRO"].fillna(0)
    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    df_dim["CD_POR_ANE"] = df_dim["CD_POR_ANE"].fillna(0)
    df_dim["DS_PRO_FAT"] = df_dim["DS_PRO_FAT"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.PRO_FAT (CD_GRU_PRO, CD_PRO_FAT, CD_POR_ANE, DS_PRO_FAT) VALUES (:1, :2, :3, :4)"

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

    print("Dados PRO_FAT inseridos")

def df_tuss():
    print("Entrou no df_tuss")

    df_dim = pd.read_sql(query_tuss, connect_rhp())

    print(df_dim)

    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    df_dim["CD_TUSS"] = df_dim["CD_TUSS"].fillna("0")
    df_dim["DS_TUSS"] = df_dim["DS_TUSS"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.TUSS (CD_TUSS, CD_PRO_FAT, DS_TUSS) VALUES (:1, :2, :3)"

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

    print("Dados TUSS inseridos")

def df_uni_pro():
    print("Entrou no df_uni_pro")

    df_dim = pd.read_sql(query_uni_pro, connect_rhp())

    print(df_dim)

    df_dim["CD_UNIDADE"] = df_dim["CD_UNIDADE"].fillna("0")
    df_dim["DS_UNIDADE"] = df_dim["DS_UNIDADE"].fillna("0")
    df_dim["VL_FATOR"] = df_dim["VL_FATOR"].fillna(0)
    df_dim["TP_RELATORIOS"] = df_dim["TP_RELATORIOS"].fillna("0")
    df_dim["CD_UNI_PRO"] = df_dim["CD_UNI_PRO"].fillna(0)
    df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
    df_dim["SN_ATIVO"] = df_dim["SN_ATIVO"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.UNI_PRO (DS_UNIDADE, CD_UNIDADE, VL_FATOR, TP_RELATORIOS, CD_UNI_PRO, CD_PRODUTO, SN_ATIVO) VALUES (:1, :2, :3, :4, :5, :6, :7)"

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

    print("Dados UNI_PRO inseridos")

def df_reg_amb():
    print("Entrou no df_reg_amb")

    df_dim = pd.read_sql(query_reg_amb, connect_rhp())

    print(df_dim)

    df_dim["CD_REG_AMB"] = df_dim["CD_REG_AMB"].fillna(0)
    df_dim["CD_REMESSA"] = df_dim["CD_REMESSA"].fillna(0)
    df_dim["VL_TOTAL_CONTA"] = df_dim["VL_TOTAL_CONTA"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.REG_AMB (CD_REG_AMB, CD_REMESSA, VL_TOTAL_CONTA) VALUES (:1, :2, :3)"

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

    print("Dados REG_AMB inseridos")

def df_itreg_amb():
    print("Entrou no df_itreg_amb")

    print("2021")
    df_dim = pd.read_sql(query_itreg_amb.format(data_ini='2021-01-01 00:00:00', data_fim='2021-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    df_dim["CD_REG_AMB"] = df_dim["CD_REG_AMB"].fillna(0)
    df_dim["CD_GRU_FAT"] = df_dim["CD_GRU_FAT"].fillna(0)
    df_dim["CD_LANCAMENTO"] = df_dim["CD_LANCAMENTO"].fillna(0)
    df_dim["QT_LANCAMENTO"] = df_dim["QT_LANCAMENTO"].fillna(0)
    df_dim["VL_UNITARIO"] = df_dim["VL_UNITARIO"].fillna(0)
    df_dim["VL_NOTA"] = df_dim["VL_NOTA"].fillna(0)
    df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)
    df_dim["CD_SETOR_PRODUZIU"] = df_dim["CD_SETOR_PRODUZIU"].fillna(0)
    df_dim["TP_PAGAMENTO"] = df_dim["TP_PAGAMENTO"].fillna("0")
    df_dim["SN_PERTENCE_PACOTE"] = df_dim["SN_PERTENCE_PACOTE"].fillna("0")
    df_dim["VL_TOTAL_CONTA"] = df_dim["VL_TOTAL_CONTA"].fillna(0)
    df_dim["SN_FECHADA"] = df_dim["SN_FECHADA"].fillna("0")
    # df_dim["DT_FECHAMENTO"] = df_dim["DT_FECHAMENTO"].fillna("01.01.1899 00:00:00")
    df_dim["CD_ITMVTO"] = df_dim["CD_ITMVTO"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ITREG_AMB (CD_ATENDIMENTO, CD_PRO_FAT, CD_REG_AMB, CD_GRU_FAT, CD_LANCAMENTO, QT_LANCAMENTO, VL_UNITARIO, VL_NOTA, CD_SETOR, CD_SETOR_PRODUZIU, TP_PAGAMENTO, SN_PERTENCE_PACOTE, VL_TOTAL_CONTA, SN_FECHADA, DT_FECHAMENTO, CD_ITMVTO) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)"

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

    print("Dados ITREG_AMB inseridos")

    print("2020")
    df_dim = pd.read_sql(query_itreg_amb.format(data_ini='2020-01-01 00:00:00', data_fim='2020-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    df_dim["CD_REG_AMB"] = df_dim["CD_REG_AMB"].fillna(0)
    df_dim["CD_GRU_FAT"] = df_dim["CD_GRU_FAT"].fillna(0)
    df_dim["CD_LANCAMENTO"] = df_dim["CD_LANCAMENTO"].fillna(0)
    df_dim["QT_LANCAMENTO"] = df_dim["QT_LANCAMENTO"].fillna(0)
    df_dim["VL_UNITARIO"] = df_dim["VL_UNITARIO"].fillna(0)
    df_dim["VL_NOTA"] = df_dim["VL_NOTA"].fillna(0)
    df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)
    df_dim["CD_SETOR_PRODUZIU"] = df_dim["CD_SETOR_PRODUZIU"].fillna(0)
    df_dim["TP_PAGAMENTO"] = df_dim["TP_PAGAMENTO"].fillna("0")
    df_dim["SN_PERTENCE_PACOTE"] = df_dim["SN_PERTENCE_PACOTE"].fillna("0")
    df_dim["VL_TOTAL_CONTA"] = df_dim["VL_TOTAL_CONTA"].fillna(0)
    df_dim["SN_FECHADA"] = df_dim["SN_FECHADA"].fillna("0")
    # df_dim["DT_FECHAMENTO"] = df_dim["DT_FECHAMENTO"].fillna("01.01.1899 00:00:00")
    df_dim["CD_ITMVTO"] = df_dim["CD_ITMVTO"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ITREG_AMB (CD_ATENDIMENTO, CD_PRO_FAT, CD_REG_AMB, CD_GRU_FAT, CD_LANCAMENTO, QT_LANCAMENTO, VL_UNITARIO, VL_NOTA, CD_SETOR, CD_SETOR_PRODUZIU, TP_PAGAMENTO, SN_PERTENCE_PACOTE, VL_TOTAL_CONTA, SN_FECHADA, DT_FECHAMENTO, CD_ITMVTO) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)"

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

    print("Dados ITREG_AMB inseridos")

    print("2019")
    df_dim = pd.read_sql(query_itreg_amb.format(data_ini='2019-01-01 00:00:00', data_fim='2019-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    df_dim["CD_REG_AMB"] = df_dim["CD_REG_AMB"].fillna(0)
    df_dim["CD_GRU_FAT"] = df_dim["CD_GRU_FAT"].fillna(0)
    df_dim["CD_LANCAMENTO"] = df_dim["CD_LANCAMENTO"].fillna(0)
    df_dim["QT_LANCAMENTO"] = df_dim["QT_LANCAMENTO"].fillna(0)
    df_dim["VL_UNITARIO"] = df_dim["VL_UNITARIO"].fillna(0)
    df_dim["VL_NOTA"] = df_dim["VL_NOTA"].fillna(0)
    df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)
    df_dim["CD_SETOR_PRODUZIU"] = df_dim["CD_SETOR_PRODUZIU"].fillna(0)
    df_dim["TP_PAGAMENTO"] = df_dim["TP_PAGAMENTO"].fillna("0")
    df_dim["SN_PERTENCE_PACOTE"] = df_dim["SN_PERTENCE_PACOTE"].fillna("0")
    df_dim["VL_TOTAL_CONTA"] = df_dim["VL_TOTAL_CONTA"].fillna(0)
    df_dim["SN_FECHADA"] = df_dim["SN_FECHADA"].fillna("0")
    # df_dim["DT_FECHAMENTO"] = df_dim["DT_FECHAMENTO"].fillna("01.01.1899 00:00:00")
    df_dim["CD_ITMVTO"] = df_dim["CD_ITMVTO"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ITREG_AMB (CD_ATENDIMENTO, CD_PRO_FAT, CD_REG_AMB, CD_GRU_FAT, CD_LANCAMENTO, QT_LANCAMENTO, VL_UNITARIO, VL_NOTA, CD_SETOR, CD_SETOR_PRODUZIU, TP_PAGAMENTO, SN_PERTENCE_PACOTE, VL_TOTAL_CONTA, SN_FECHADA, DT_FECHAMENTO, CD_ITMVTO) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)"

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

    print("Dados ITREG_AMB inseridos")

def df_reg_fat():
    print("Entrou no df_reg_fat")

    print("2021")
    df_dim = pd.read_sql(query_reg_fat.format(data_ini='2021-01-01 00:00:00', data_fim='2021-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_REG_FAT"] = df_dim["CD_REG_FAT"].fillna(0)
    df_dim["SN_FECHADA"] = df_dim["SN_FECHADA"].fillna("0")
    # df_dim["DT_INICIO"] = df_dim["DT_INICIO"].fillna("01.01.1899 00:00:00")
    # df_dim["DT_FINAL"] = df_dim["DT_FINAL"].fillna("01.01.1899 00:00:00")
    # df_dim["DT_FECHAMENTO"] = df_dim["DT_FECHAMENTO"].fillna("01.01.1899 00:00:00")
    df_dim["CD_REMESSA"] = df_dim["CD_REMESSA"].fillna(0)
    df_dim["VL_TOTAL_CONTA"] = df_dim["VL_TOTAL_CONTA"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.REG_FAT (CD_REG_FAT, SN_FECHADA, DT_INICIO, DT_FINAL, DT_FECHAMENTO, CD_REMESSA, VL_TOTAL_CONTA, CD_ATENDIMENTO) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"

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

    print("Dados REG_FAT inseridos")

    print("2020")
    df_dim = pd.read_sql(query_reg_fat.format(data_ini='2020-01-01 00:00:00', data_fim='2020-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_REG_FAT"] = df_dim["CD_REG_FAT"].fillna(0)
    df_dim["SN_FECHADA"] = df_dim["SN_FECHADA"].fillna("0")
    # df_dim["DT_INICIO"] = df_dim["DT_INICIO"].fillna("01.01.1899 00:00:00")
    # df_dim["DT_FINAL"] = df_dim["DT_FINAL"].fillna("01.01.1899 00:00:00")
    # df_dim["DT_FECHAMENTO"] = df_dim["DT_FECHAMENTO"].fillna("01.01.1899 00:00:00")
    df_dim["CD_REMESSA"] = df_dim["CD_REMESSA"].fillna(0)
    df_dim["VL_TOTAL_CONTA"] = df_dim["VL_TOTAL_CONTA"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.REG_FAT (CD_REG_FAT, SN_FECHADA, DT_INICIO, DT_FINAL, DT_FECHAMENTO, CD_REMESSA, VL_TOTAL_CONTA, CD_ATENDIMENTO) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"

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

    print("Dados REG_FAT inseridos")

    print("2019")
    df_dim = pd.read_sql(query_reg_fat.format(data_ini='2019-01-01 00:00:00', data_fim='2019-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_REG_FAT"] = df_dim["CD_REG_FAT"].fillna(0)
    df_dim["SN_FECHADA"] = df_dim["SN_FECHADA"].fillna("0")
    # df_dim["DT_INICIO"] = df_dim["DT_INICIO"].fillna("01.01.1899 00:00:00")
    # df_dim["DT_FINAL"] = df_dim["DT_FINAL"].fillna("01.01.1899 00:00:00")
    # df_dim["DT_FECHAMENTO"] = df_dim["DT_FECHAMENTO"].fillna("01.01.1899 00:00:00")
    df_dim["CD_REMESSA"] = df_dim["CD_REMESSA"].fillna(0)
    df_dim["VL_TOTAL_CONTA"] = df_dim["VL_TOTAL_CONTA"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.REG_FAT (CD_REG_FAT, SN_FECHADA, DT_INICIO, DT_FINAL, DT_FECHAMENTO, CD_REMESSA, VL_TOTAL_CONTA, CD_ATENDIMENTO) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"

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

    print("Dados REG_FAT inseridos")

def df_itreg_fat():
    print("Entrou no df_itreg_fat")

    print("2021")
    df_dim = pd.read_sql(query_itreg_fat.format(data_ini='2021-01-01 00:00:00', data_fim='2021-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_REG_FAT"] = df_dim["CD_REG_FAT"].fillna(0)
    df_dim["CD_LANCAMENTO"] = df_dim["CD_LANCAMENTO"].fillna(0)
    # df_dim["DT_LANCAMENTO"] = df_dim["DT_LANCAMENTO"].fillna("01.01.1899 00:00:00")
    df_dim["QT_LANCAMENTO"] = df_dim["QT_LANCAMENTO"].fillna(0)
    df_dim["TP_PAGAMENTO"] = df_dim["TP_PAGAMENTO"].fillna("0")
    df_dim["VL_UNITARIO"] = df_dim["VL_UNITARIO"].fillna(0)
    df_dim["VL_NOTA"] = df_dim["VL_NOTA"].fillna(0)
    df_dim["CD_CONTA_PAI"] = df_dim["CD_CONTA_PAI"].fillna(0)
    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    df_dim["CD_GRU_FAT"] = df_dim["CD_GRU_FAT"].fillna(0)
    df_dim["VL_TOTAL_CONTA"] = df_dim["VL_TOTAL_CONTA"].fillna(0)
    df_dim["SN_PERTENCE_PACOTE"] = df_dim["SN_PERTENCE_PACOTE"].fillna("0")
    df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)
    df_dim["CD_SETOR_PRODUZIU"] = df_dim["CD_SETOR_PRODUZIU"].fillna(0)
    df_dim["CD_ITMVTO"] = df_dim["CD_ITMVTO"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ITREG_FAT (CD_REG_FAT, CD_LANCAMENTO, DT_LANCAMENTO, QT_LANCAMENTO, TP_PAGAMENTO, VL_UNITARIO, VL_NOTA, CD_CONTA_PAI, CD_PRO_FAT, CD_PRO_FAT, CD_GRU_FAT, VL_TOTAL_CONTA, SN_PERTENCE_PACOTE, CD_SETOR, CD_SETOR_PRODUZIU, CD_ITMVTO) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)"

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

    print("Dados ITREG_FAT inseridos")

    print("2020")
    df_dim = pd.read_sql(query_itreg_fat.format(data_ini='2020-01-01 00:00:00', data_fim='2020-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_REG_FAT"] = df_dim["CD_REG_FAT"].fillna(0)
    df_dim["CD_LANCAMENTO"] = df_dim["CD_LANCAMENTO"].fillna(0)
    # df_dim["DT_LANCAMENTO"] = df_dim["DT_LANCAMENTO"].fillna("01.01.1899 00:00:00")
    df_dim["QT_LANCAMENTO"] = df_dim["QT_LANCAMENTO"].fillna(0)
    df_dim["TP_PAGAMENTO"] = df_dim["TP_PAGAMENTO"].fillna("0")
    df_dim["VL_UNITARIO"] = df_dim["VL_UNITARIO"].fillna(0)
    df_dim["VL_NOTA"] = df_dim["VL_NOTA"].fillna(0)
    df_dim["CD_CONTA_PAI"] = df_dim["CD_CONTA_PAI"].fillna(0)
    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    df_dim["CD_GRU_FAT"] = df_dim["CD_GRU_FAT"].fillna(0)
    df_dim["VL_TOTAL_CONTA"] = df_dim["VL_TOTAL_CONTA"].fillna(0)
    df_dim["SN_PERTENCE_PACOTE"] = df_dim["SN_PERTENCE_PACOTE"].fillna("0")
    df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)
    df_dim["CD_SETOR_PRODUZIU"] = df_dim["CD_SETOR_PRODUZIU"].fillna(0)
    df_dim["CD_ITMVTO"] = df_dim["CD_ITMVTO"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ITREG_FAT (CD_REG_FAT, CD_LANCAMENTO, DT_LANCAMENTO, QT_LANCAMENTO, TP_PAGAMENTO, VL_UNITARIO, VL_NOTA, CD_CONTA_PAI, CD_PRO_FAT, CD_PRO_FAT, CD_GRU_FAT, VL_TOTAL_CONTA, SN_PERTENCE_PACOTE, CD_SETOR, CD_SETOR_PRODUZIU, CD_ITMVTO) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)"

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

    print("Dados ITREG_FAT inseridos")

    print("2019")
    df_dim = pd.read_sql(query_itreg_fat.format(data_ini='2019-01-01 00:00:00', data_fim='2019-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_REG_FAT"] = df_dim["CD_REG_FAT"].fillna(0)
    df_dim["CD_LANCAMENTO"] = df_dim["CD_LANCAMENTO"].fillna(0)
    # df_dim["DT_LANCAMENTO"] = df_dim["DT_LANCAMENTO"].fillna("01.01.1899 00:00:00")
    df_dim["QT_LANCAMENTO"] = df_dim["QT_LANCAMENTO"].fillna(0)
    df_dim["TP_PAGAMENTO"] = df_dim["TP_PAGAMENTO"].fillna("0")
    df_dim["VL_UNITARIO"] = df_dim["VL_UNITARIO"].fillna(0)
    df_dim["VL_NOTA"] = df_dim["VL_NOTA"].fillna(0)
    df_dim["CD_CONTA_PAI"] = df_dim["CD_CONTA_PAI"].fillna(0)
    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    df_dim["CD_GRU_FAT"] = df_dim["CD_GRU_FAT"].fillna(0)
    df_dim["VL_TOTAL_CONTA"] = df_dim["VL_TOTAL_CONTA"].fillna(0)
    df_dim["SN_PERTENCE_PACOTE"] = df_dim["SN_PERTENCE_PACOTE"].fillna("0")
    df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)
    df_dim["CD_SETOR_PRODUZIU"] = df_dim["CD_SETOR_PRODUZIU"].fillna(0)
    df_dim["CD_ITMVTO"] = df_dim["CD_ITMVTO"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ITREG_FAT (CD_REG_FAT, CD_LANCAMENTO, DT_LANCAMENTO, QT_LANCAMENTO, TP_PAGAMENTO, VL_UNITARIO, VL_NOTA, CD_CONTA_PAI, CD_PRO_FAT, CD_PRO_FAT, CD_GRU_FAT, VL_TOTAL_CONTA, SN_PERTENCE_PACOTE, CD_SETOR, CD_SETOR_PRODUZIU, CD_ITMVTO) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)"

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

    print("Dados ITREG_FAT inseridos")

def df_custo_final():
    print("Entrou no df_custo_final")

    print("2021")
    df_dim = pd.read_sql(query_custo_final.format(data_ini='2021-01-01 00:00:00', data_fim='2021-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["VL_CUSTO_CENCIR"] = df_dim["VL_CUSTO_CENCIR"].fillna(0)
    # df_dim["DT_COMPETENCIA"] = df_dim["DT_COMPETENCIA"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.CUSTO_FINAL (VL_CUSTO_CENCIR, DT_COMPETENCIA) VALUES (:1, :2)"

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

    print("Dados CUSTO_FINAL inseridos")

    print("2020")
    df_dim = pd.read_sql(query_custo_final.format(data_ini='2020-01-01 00:00:00', data_fim='2020-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["VL_CUSTO_CENCIR"] = df_dim["VL_CUSTO_CENCIR"].fillna(0)
    # df_dim["DT_COMPETENCIA"] = df_dim["DT_COMPETENCIA"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.CUSTO_FINAL (VL_CUSTO_CENCIR, DT_COMPETENCIA) VALUES (:1, :2)"

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

    print("Dados CUSTO_FINAL inseridos")

    print("2019")
    df_dim = pd.read_sql(query_custo_final.format(data_ini='2019-01-01 00:00:00', data_fim='2019-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["VL_CUSTO_CENCIR"] = df_dim["VL_CUSTO_CENCIR"].fillna(0)
    # df_dim["DT_COMPETENCIA"] = df_dim["DT_COMPETENCIA"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.CUSTO_FINAL (VL_CUSTO_CENCIR, DT_COMPETENCIA) VALUES (:1, :2)"

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

    print("Dados CUSTO_FINAL inseridos")

def df_mvto_estoque():
    print("Entrou no df_mvto_estoque")

    print("2021")
    df_dim = pd.read_sql(query_mvto_estoque.format(data_ini='2021-01-01 00:00:00', data_fim='2021-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_MVTO_ESTOQUE"] = df_dim["CD_MVTO_ESTOQUE"].fillna(0)
    df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_MOT_DEV"] = df_dim["CD_MOT_DEV"].fillna(0)
    df_dim["CD_MULTI_EMPRESA"] = df_dim["CD_MULTI_EMPRESA"].fillna(0)
    # df_dim["DT_MVTO_ESTOQUE"] = df_dim["DT_MVTO_ESTOQUE"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.MVTO_ESTOQUE (CD_MVTO_ESTOQUE, CD_SETOR, CD_ATENDIMENTO, CD_MOT_DEV, CD_MULTI_EMPRESA, DT_MVTO_ESTOQUE) VALUES (:1, :2, :3, :4, :5, :6)"

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

    print("Dados MVTO_ESTOQUE inseridos")

    print("2020")
    df_dim = pd.read_sql(query_mvto_estoque.format(data_ini='2020-01-01 00:00:00', data_fim='2020-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_MVTO_ESTOQUE"] = df_dim["CD_MVTO_ESTOQUE"].fillna(0)
    df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_MOT_DEV"] = df_dim["CD_MOT_DEV"].fillna(0)
    df_dim["CD_MULTI_EMPRESA"] = df_dim["CD_MULTI_EMPRESA"].fillna(0)
    # df_dim["DT_MVTO_ESTOQUE"] = df_dim["DT_MVTO_ESTOQUE"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.MVTO_ESTOQUE (CD_MVTO_ESTOQUE, CD_SETOR, CD_ATENDIMENTO, CD_MOT_DEV, CD_MULTI_EMPRESA, DT_MVTO_ESTOQUE) VALUES (:1, :2, :3, :4, :5, :6)"

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

    print("Dados MVTO_ESTOQUE inseridos")

    print("2019")
    df_dim = pd.read_sql(query_mvto_estoque.format(data_ini='2019-01-01 00:00:00', data_fim='2019-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_MVTO_ESTOQUE"] = df_dim["CD_MVTO_ESTOQUE"].fillna(0)
    df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_MOT_DEV"] = df_dim["CD_MOT_DEV"].fillna(0)
    df_dim["CD_MULTI_EMPRESA"] = df_dim["CD_MULTI_EMPRESA"].fillna(0)
    # df_dim["DT_MVTO_ESTOQUE"] = df_dim["DT_MVTO_ESTOQUE"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.MVTO_ESTOQUE (CD_MVTO_ESTOQUE, CD_SETOR, CD_ATENDIMENTO, CD_MOT_DEV, CD_MULTI_EMPRESA, DT_MVTO_ESTOQUE) VALUES (:1, :2, :3, :4, :5, :6)"

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

    print("Dados MVTO_ESTOQUE inseridos")

def df_itmvto_estoque():
    print("Entrou no df_itmvto_estoque")

    df_dim = pd.read_sql(query_itmvto_estoque, connect_rhp())

    print(df_dim)

    df_dim["CD_ITMVTO_ESTOQUE"] = df_dim["CD_ITMVTO_ESTOQUE"].fillna(0)
    df_dim["QT_MOVIMENTACAO"] = df_dim["QT_MOVIMENTACAO"].fillna(0)
    df_dim["CD_MVTO_ESTOQUE"] = df_dim["CD_MVTO_ESTOQUE"].fillna(0)
    df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
    df_dim["CD_UNI_PRO"] = df_dim["CD_UNI_PRO"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ITMVTO_ESTOQUE (CD_ITMVTO_ESTOQUE, QT_MOVIMENTACAO, CD_MVTO_ESTOQUE, CD_PRODUTO, CD_UNI_PRO) VALUES (:1, :2, :3, :4, :5)"

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

    print("Dados ITMVTO_ESTOQUE inseridos")

def df_quantidade_diarias():
    print("Entrou no df_quantidade_diarias")

    df_dim = pd.read_sql(query_quantidade_diarias, connect_rhp())

    print(df_dim)

    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["VL_DIARIA"] = df_dim["VL_DIARIA"].fillna(0)
    df_dim["QTD_DIARIAS"] = df_dim["QTD_DIARIAS"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.QUANTIDADE_DIARIAS (CD_ATENDIMENTO, VL_DIARIA, QTD_DIARIAS) VALUES (:1, :2, :3)"

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

    print("Dados QUANTIDADE_DIARIAS inseridos")

def df_remessa_fatura():
    print("Entrou no df_remessa_fatura")

    print("2021")
    df_dim = pd.read_sql(query_remessa_fatura.format(data_ini='2021-01-01 00:00:00', data_fim='2021-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_REMESSA"] = df_dim["CD_REMESSA"].fillna(0)
    # df_dim["DT_ABERTURA"] = df_dim["DT_ABERTURA"].fillna("01.01.1899 00:00:00")
    # df_dim["DT_FECHAMENTO"] = df_dim["DT_FECHAMENTO"].fillna("01.01.1899 00:00:00")
    # df_dim["DT_ENTREGA_DA_FATURA"] = df_dim["DT_ENTREGA_DA_FATURA"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.REMESSA_FATURA (CD_REMESSA, DT_ABERTURA, DT_FECHAMENTO, DT_ENTREGA_DA_FATURA) VALUES (:1, :2, :3, :4)"

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

    print("Dados REMESSA_FATURA inseridos")

    print("2020")
    df_dim = pd.read_sql(query_remessa_fatura.format(data_ini='2020-01-01 00:00:00', data_fim='2020-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_REMESSA"] = df_dim["CD_REMESSA"].fillna(0)
    # df_dim["DT_ABERTURA"] = df_dim["DT_ABERTURA"].fillna("01.01.1899 00:00:00")
    # df_dim["DT_FECHAMENTO"] = df_dim["DT_FECHAMENTO"].fillna("01.01.1899 00:00:00")
    # df_dim["DT_ENTREGA_DA_FATURA"] = df_dim["DT_ENTREGA_DA_FATURA"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.REMESSA_FATURA (CD_REMESSA, DT_ABERTURA, DT_FECHAMENTO, DT_ENTREGA_DA_FATURA) VALUES (:1, :2, :3, :4)"

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

    print("Dados REMESSA_FATURA inseridos")

    print("2019")
    df_dim = pd.read_sql(query_remessa_fatura.format(data_ini='2019-01-01 00:00:00', data_fim='2019-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_REMESSA"] = df_dim["CD_REMESSA"].fillna(0)
    # df_dim["DT_ABERTURA"] = df_dim["DT_ABERTURA"].fillna("01.01.1899 00:00:00")
    # df_dim["DT_FECHAMENTO"] = df_dim["DT_FECHAMENTO"].fillna("01.01.1899 00:00:00")
    # df_dim["DT_ENTREGA_DA_FATURA"] = df_dim["DT_ENTREGA_DA_FATURA"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.REMESSA_FATURA (CD_REMESSA, DT_ABERTURA, DT_FECHAMENTO, DT_ENTREGA_DA_FATURA) VALUES (:1, :2, :3, :4)"

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

    print("Dados REMESSA_FATURA inseridos")

def df_repasse():
    print("Entrou no df_repasse")

    print("2021")
    df_dim = pd.read_sql(query_repasse.format(data_ini='2021-01-01 00:00:00', data_fim='2021-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_REPASSE"] = df_dim["CD_REPASSE"].fillna(0)
    # df_dim["DT_COMPETENCIA"] = df_dim["DT_COMPETENCIA"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.REPASSE (CD_REPASSE, DT_COMPETENCIA) VALUES (:1, :2)"

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

    print("Dados REPASSE inseridos")

    print("2020")
    df_dim = pd.read_sql(query_repasse.format(data_ini='2020-01-01 00:00:00', data_fim='2020-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_REPASSE"] = df_dim["CD_REPASSE"].fillna(0)
    # df_dim["DT_COMPETENCIA"] = df_dim["DT_COMPETENCIA"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.REPASSE (CD_REPASSE, DT_COMPETENCIA) VALUES (:1, :2)"

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

    print("Dados REPASSE inseridos")

    print("2019")
    df_dim = pd.read_sql(query_repasse.format(data_ini='2019-01-01 00:00:00', data_fim='2019-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["CD_REPASSE"] = df_dim["CD_REPASSE"].fillna(0)
    # df_dim["DT_COMPETENCIA"] = df_dim["DT_COMPETENCIA"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.REPASSE (CD_REPASSE, DT_COMPETENCIA) VALUES (:1, :2)"

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

    print("Dados REPASSE inseridos")

def df_it_repasse():
    print("Entrou no df_it_repasse")

    df_dim = pd.read_sql(query_it_repasse, connect_rhp())

    print(df_dim)

    df_dim["CD_REG_FAT"] = df_dim["CD_REG_FAT"].fillna(0)
    df_dim["CD_LANCAMENTO_FAT"] = df_dim["CD_LANCAMENTO_FAT"].fillna(0)
    df_dim["CD_REPASSE"] = df_dim["CD_REPASSE"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.IT_REPASSE (CD_REG_FAT, CD_LANCAMENTO_FAT, CD_REPASSE) VALUES (:1, :2, :3)"

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

    print("Dados IT_REPASSE inseridos")

def df_itent_pro():
    print("Entrou no df_itent_pro")

    print("2021")
    df_dim = pd.read_sql(query_itent_pro.format(data_ini='2021-01-01 00:00:00', data_fim='2021-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["VL_TOTAL"] = df_dim["VL_TOTAL"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
    df_dim["VL_UNITARIO"] = df_dim["VL_UNITARIO"].fillna(0)
    # df_dim["DT_GRAVACAO"] = df_dim["DT_GRAVACAO"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ITENT_PRO (VL_TOTAL, CD_ATENDIMENTO, CD_PRODUTO, VL_UNITARIO, DT_GRAVACAO) VALUES (:1, :2, :3, :4, :5)"

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

    print("Dados ITENT_PRO inseridos")

    print("2020")
    df_dim = pd.read_sql(query_itent_pro.format(data_ini='2020-01-01 00:00:00', data_fim='2020-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["VL_TOTAL"] = df_dim["VL_TOTAL"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
    df_dim["VL_UNITARIO"] = df_dim["VL_UNITARIO"].fillna(0)
    # df_dim["DT_GRAVACAO"] = df_dim["DT_GRAVACAO"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ITENT_PRO (VL_TOTAL, CD_ATENDIMENTO, CD_PRODUTO, VL_UNITARIO, DT_GRAVACAO) VALUES (:1, :2, :3, :4, :5)"

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

    print("Dados ITENT_PRO inseridos")

    print("2019")
    df_dim = pd.read_sql(query_itent_pro.format(data_ini='2019-01-01 00:00:00', data_fim='2019-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["VL_TOTAL"] = df_dim["VL_TOTAL"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
    df_dim["VL_UNITARIO"] = df_dim["VL_UNITARIO"].fillna(0)
    # df_dim["DT_GRAVACAO"] = df_dim["DT_GRAVACAO"].fillna("01.01.1899 00:00:00")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ITENT_PRO (VL_TOTAL, CD_ATENDIMENTO, CD_PRODUTO, VL_UNITARIO, DT_GRAVACAO) VALUES (:1, :2, :3, :4, :5)"

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

    print("Dados ITENT_PRO inseridos")

def df_glosas():
    print("Entrou no df_glosas")

    df_dim = pd.read_sql(query_glosas, connect_rhp())

    print(df_dim)

    df_dim["CD_GLOSAS"] = df_dim["CD_GLOSAS"].fillna(0)
    df_dim["CD_REG_FAT"] = df_dim["CD_REG_FAT"].fillna(0)
    df_dim["CD_REG_AMB"] = df_dim["CD_REG_AMB"].fillna(0)
    df_dim["CD_MOTIVO_GLOSA"] = df_dim["CD_MOTIVO_GLOSA"].fillna(0)
    df_dim["VL_GLOSA"] = df_dim["VL_GLOSA"].fillna(0)
    df_dim["CD_LANCAMENTO_FAT"] = df_dim["CD_LANCAMENTO_FAT"].fillna(0)
    df_dim["CD_LANCAMENTO_AMB"] = df_dim["CD_LANCAMENTO_AMB"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.GLOSAS (CD_GLOSAS, CD_REG_FAT, CD_REG_AMB, CD_MOTIVO_GLOSA, VL_GLOSA, CD_LANCAMENTO_FAT, CD_LANCAMENTO_AMB) VALUES (:1, :2, :3, :4, :5, :6, :7)"

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

    print("Dados GLOSAS inseridos")

def df_custo_medio_mensal():
    print("Entrou no df_custo_medio_mensal")

    print("2021")
    df_dim = pd.read_sql(query_custo_medio_mensal.format(data_ini='2021-01-01 00:00:00', data_fim='2021-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["VL_CUSTO_MEDIO"] = df_dim["VL_CUSTO_MEDIO"].fillna(0)
    # df_dim["DH_CUSTO_MEDIO"] = df_dim["DH_CUSTO_MEDIO"].fillna("01.01.1899 00:00:00")
    df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
    df_dim["CD_MULTI_EMPRESA"] = df_dim["CD_MULTI_EMPRESA"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.CUSTO_MEDIO_MENSAL (VL_CUSTO_MEDIO, DH_CUSTO_MEDIO, CD_PRODUTO, CD_MULTI_EMPRESA) VALUES (:1, :2, :3, :4)"

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

    print("Dados CUSTO_MEDIO_MENSAL inseridos")

    print("2020")
    df_dim = pd.read_sql(query_custo_medio_mensal.format(data_ini='2020-01-01 00:00:00', data_fim='2020-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["VL_CUSTO_MEDIO"] = df_dim["VL_CUSTO_MEDIO"].fillna(0)
    # df_dim["DH_CUSTO_MEDIO"] = df_dim["DH_CUSTO_MEDIO"].fillna("01.01.1899 00:00:00")
    df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
    df_dim["CD_MULTI_EMPRESA"] = df_dim["CD_MULTI_EMPRESA"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.CUSTO_MEDIO_MENSAL (VL_CUSTO_MEDIO, DH_CUSTO_MEDIO, CD_PRODUTO, CD_MULTI_EMPRESA) VALUES (:1, :2, :3, :4)"

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

    print("Dados CUSTO_MEDIO_MENSAL inseridos")

    print("2019")
    df_dim = pd.read_sql(query_custo_medio_mensal.format(data_ini='2019-01-01 00:00:00', data_fim='2019-12-31 23:59:59'), connect_rhp())

    print(df_dim)

    df_dim["VL_CUSTO_MEDIO"] = df_dim["VL_CUSTO_MEDIO"].fillna(0)
    # df_dim["DH_CUSTO_MEDIO"] = df_dim["DH_CUSTO_MEDIO"].fillna("01.01.1899 00:00:00")
    df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
    df_dim["CD_MULTI_EMPRESA"] = df_dim["CD_MULTI_EMPRESA"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.CUSTO_MEDIO_MENSAL (VL_CUSTO_MEDIO, DH_CUSTO_MEDIO, CD_PRODUTO, CD_MULTI_EMPRESA) VALUES (:1, :2, :3, :4)"

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

    print("Dados CUSTO_MEDIO_MENSAL inseridos")

def df_fa_custo_atendimento():
    print("Entrou no df_fa_custo_atendimento")

    df_dim = pd.read_sql(query_fa_custo_atendimento, connect_rhp())

    print(df_dim)

    df_dim["VL_DIARIA"] = df_dim["VL_DIARIA"].fillna(0)
    df_dim["VL_CUSTO_GASES"] = df_dim["VL_CUSTO_GASES"].fillna(0)
    df_dim["VL_CUSTO_REPASSE"] = df_dim["VL_CUSTO_REPASSE"].fillna(0)
    df_dim["VL_CUSTO_MEDICAMENTO"] = df_dim["VL_CUSTO_MEDICAMENTO"].fillna(0)
    df_dim["VL_PROCEDIMENTO"] = df_dim["VL_PROCEDIMENTO"].fillna(0)
    df_dim["VL_CUSTO_DIARIATAXA"] = df_dim["VL_CUSTO_DIARIATAXA"].fillna(0)
    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.FA_CUSTO_ATENDIMENTO (VL_DIARIA, VL_CUSTO_GASES, VL_CUSTO_REPASSE, VL_CUSTO_MEDICAMENTO, VL_PROCEDIMENTO, VL_CUSTO_DIARIATAXA, CD_ATENDIMENTO) VALUES (:1, :2, :3, :4, :5, :6, :7)"

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

    print("Dados FA_CUSTO_ATENDIMENTO inseridos")

def df_especie():
    print("Entrou no df_especie")

    df_dim = pd.read_sql(query_especie, connect_rhp())

    print(df_dim)

    df_dim["CD_ESPECIE"] = df_dim["CD_ESPECIE"].fillna(0)
    df_dim["DS_ESPECIE"] = df_dim["DS_ESPECIE"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ESPECIE (CD_ESPECIE, DS_ESPECIE) VALUES (:1, :2)"

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

    print("Dados ESPECIE inseridos")

def df_exa_lab():
    print("Entrou no df_exa_lab")

    df_dim = pd.read_sql(query_exa_lab, connect_rhp())

    print(df_dim)

    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    df_dim["CD_EXA_LAB"] = df_dim["CD_EXA_LAB"].fillna(0)
    df_dim["NM_EXA_LAB"] = df_dim["NM_EXA_LAB"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.EXA_LAB (CD_PRO_FAT, CD_EXA_LAB, NM_EXA_LAB) VALUES (:1, :2, :3)"

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

    print("Dados EXA_LAB inseridos")

def df_exa_rx():
    print("Entrou no df_exa_rx")

    df_dim = pd.read_sql(query_exa_rx, connect_rhp())

    print(df_dim)

    df_dim["EXA_RX_CD_PRO_FAT"] = df_dim["EXA_RX_CD_PRO_FAT"].fillna("0")
    df_dim["CD_EXA_RX"] = df_dim["CD_EXA_RX"].fillna(0)
    df_dim["DS_EXA_RX"] = df_dim["DS_EXA_RX"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.EXA_RX (EXA_RX_CD_PRO_FAT, CD_EXA_RX, DS_EXA_RX) VALUES (:1, :2, :3)"

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

    print("Dados EXA_RX inseridos")

def df_motivo_glosa():
    print("Entrou no df_motivo_glosa")

    df_dim = pd.read_sql(query_motivo_glosa, connect_rhp())

    print(df_dim)

    df_dim["DS_MOTIVO_GLOSA"] = df_dim["DS_MOTIVO_GLOSA"].fillna("0")
    df_dim["CD_MOTIVO_GLOSA"] = df_dim["CD_MOTIVO_GLOSA"].fillna(0)
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.MOTIVO_GLOSA (DS_MOTIVO_GLOSA, CD_MOTIVO_GLOSA) VALUES (:1, :2)"

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

    print("Dados MOTIVO_GLOSA inseridos")

def df_mot_dev():
    print("Entrou no df_mot_dev")

    df_dim = pd.read_sql(query_mot_dev, connect_rhp())

    print(df_dim)

    df_dim["CD_MOT_DEV"] = df_dim["CD_MOT_DEV"].fillna(0)
    df_dim["DS_MOT_DEV"] = df_dim["DS_MOT_DEV"].fillna("0")
    
    print("dados para incremento")
    print(df_dim.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.MOT_DEV (CD_MOT_DEV, DS_MOT_DEV) VALUES (:1, :2)"

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

    print("Dados MOT_DEV inseridos")

dt = datetime.datetime.today() - datetime.timedelta(days=1)

# dag = DAG("insert_dados_rhp", default_args=default_args, schedule_interval=None)
dag = DAG("insert_dados_rhp", default_args=default_args, schedule_interval="0 7 * * 1-5")

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

t6 = PythonOperator(
    task_id="insert_diagnostico_atendime_rhp",
    python_callable=df_diagnostico_atendime,
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

# t25 = PythonOperator(
#     task_id="insert_pre_med_rhp",
#     python_callable=df_pre_med,
#     dag=dag)

# t26 = PythonOperator(
#     task_id="insert_itpre_med_rhp",
#     python_callable=df_itpre_med,
#     dag=dag)

# t27 = PythonOperator(
#     task_id="insert_tip_presc_rhp",
#     python_callable=df_tip_presc,
#     dag=dag)

# t28 = PythonOperator(
#     task_id="insert_for_apl_rhp",
#     python_callable=df_for_apl,
#     dag=dag)

# t29 = PythonOperator(
#     task_id="insert_tip_esq_rhp",
#     python_callable=df_tip_esq,
#     dag=dag)

# t30 = PythonOperator(
#     task_id="insert_tip_fre_rhp",
#     python_callable=df_tip_fre,
#     dag=dag)

# t32 = PythonOperator(
#     task_id="insert_gru_pro_rhp",
#     python_callable=df_gru_pro,
#     dag=dag)

# t33 = PythonOperator(
#     task_id="insert_produto_rhp",
#     python_callable=df_produto,
#     dag=dag)

# t34 = PythonOperator(
#     task_id="insert_pro_fat_rhp",
#     python_callable=df_pro_fat,
#     dag=dag)

# t35 = PythonOperator(
#     task_id="insert_tuss_rhp",
#     python_callable=df_tuss,
#     dag=dag)

# t36 = PythonOperator(
#     task_id="insert_uni_pro_rhp",
#     python_callable=df_uni_pro,
#     dag=dag)

# t37 = PythonOperator(
#     task_id="insert_reg_amb_rhp",
#     python_callable=df_reg_amb,
#     dag=dag)

# t38 = PythonOperator(
#     task_id="insert_itreg_amb_rhp",
#     python_callable=df_itreg_amb,
#     dag=dag)

# t39 = PythonOperator(
#     task_id="insert_reg_fat_rhp",
#     python_callable=df_reg_fat,
#     dag=dag)

# t40 = PythonOperator(
#     task_id="insert_itreg_fat_rhp",
#     python_callable=df_itreg_fat,
#     dag=dag)

# t41 = PythonOperator(
#     task_id="insert_custo_final_rhp",
#     python_callable=df_custo_final,
#     dag=dag)

# t42 = PythonOperator(
#     task_id="insert_mvto_estoque_rhp",
#     python_callable=df_mvto_estoque,
#     dag=dag)

# t43 = PythonOperator(
#     task_id="insert_itmvto_estoque_rhp",
#     python_callable=df_itmvto_estoque,
#     dag=dag)

# t44 = PythonOperator(
#     task_id="insert_quantidade_diarias_rhp",
#     python_callable=df_quantidade_diarias,
#     dag=dag)

# t45 = PythonOperator(
#     task_id="insert_remessa_fatura_rhp",
#     python_callable=df_remessa_fatura,
#     dag=dag)

# t46 = PythonOperator(
#     task_id="insert_repasse_rhp",
#     python_callable=df_repasse,
#     dag=dag)

# t47 = PythonOperator(
#     task_id="insert_it_repasse_rhp",
#     python_callable=df_it_repasse,
#     dag=dag)

# t48 = PythonOperator(
#     task_id="insert_itent_pro_rhp",
#     python_callable=df_itent_pro,
#     dag=dag)

# t49 = PythonOperator(
#     task_id="insert_glosas_rhp",
#     python_callable=df_glosas,
#     dag=dag)

# t50 = PythonOperator(
#     task_id="insert_custo_medio_mensal_rhp",
#     python_callable=df_custo_medio_mensal,
#     dag=dag)

# t51 = PythonOperator(
#     task_id="insert_fa_custo_atendimento_rhp",
#     python_callable=df_fa_custo_atendimento,
#     dag=dag)

# t52 = PythonOperator(
#     task_id="insert_especie_rhp",
#     python_callable=df_especie,
#     dag=dag)

# t53 = PythonOperator(
#     task_id="insert_exa_lab_rhp",
#     python_callable=df_exa_lab,
#     dag=dag)

# t54 = PythonOperator(
#     task_id="insert_exa_rx_rhp",
#     python_callable=df_exa_rx,
#     dag=dag)

# t55 = PythonOperator(
#     task_id="insert_gru_fat_rhp",
#     python_callable=df_gru_fat,
#     dag=dag)

# t56 = PythonOperator(
#     task_id="insert_motivo_glosa_rhp",
#     python_callable=df_motivo_glosa,
#     dag=dag)

# t57 = PythonOperator(
#     task_id="insert_mot_dev_rhp",
#     python_callable=df_mot_dev,
#     dag=dag)

(t1, t3, t4, t5, t8, t9, t10, t12, t13, t14, t15, t17, t18, t19, t21, t22, t24) >> t16 >> t23 >> t20 >> t7 >> t2 >> t0
# t5 >> t3 >> t2