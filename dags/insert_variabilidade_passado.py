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
from queries.rhp.queries import *
from queries.rhp.queries_hdata import *

from utils.integrity_checker import notify_email

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

def df_pre_med():
    print("Entrou no df_pre_med")
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2021, 4, 1), until=datetime.datetime(2021, 4, 30)):

        if dt.month == 12:
            data_fim = datetime.datetime(dt.year + 1, 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year + 1, 1, 1)
        else:
            data_fim = datetime.datetime(dt.year, dt.month + 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year, dt.month + 1, 1)

        print(dt.strftime('%d/%m/%Y'), ' a ', data_fim.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_pre_med.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp())

        df_dim["CD_PRE_MED"] = df_dim["CD_PRE_MED"].fillna(0)
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
        df_dim["CD_PRESTADOR"] = df_dim["CD_PRESTADOR"].fillna(0)
        df_dim["CD_DOCUMENTO_CLINICO"] = df_dim["CD_DOCUMENTO_CLINICO"].fillna(0)
        df_dim["TP_PRE_MED"] = df_dim["TP_PRE_MED"].fillna("0")
        df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)

        print(df_dim.info())

        lista_cds_pre_med = df_dim['CD_PRE_MED'].to_list()
        lista_cds_pre_med = [str(cd) for cd in lista_cds_pre_med]

        df_stage = pd.read_sql(query_pre_med_hdata.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage['CD_PRE_MED'],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)
        
        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.PRE_MED (CD_PRE_MED, CD_ATENDIMENTO, CD_PRESTADOR, CD_DOCUMENTO_CLINICO, DT_PRE_MED, TP_PRE_MED, CD_SETOR, HR_PRE_MED) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"

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

        df_itpre_med(lista_cds_pre_med)

def df_itpre_med(lista_cds_pre_med):
    print("Entrou no df_itpre_med")

    lista_cds_pre_med_dividida = np.array_split(lista_cds_pre_med, round(len(lista_cds_pre_med)/900) + 1)

    for cds in lista_cds_pre_med_dividida:
        cd_pre_med = ','.join(cds)

        df_dim = pd.read_sql(query_itpre_med.format(cd_pre_med=cd_pre_med), connect_rhp())

        df_dim["CD_PRE_MED"] = df_dim["CD_PRE_MED"].fillna(0)
        df_dim["CD_ITPRE_MED"] = df_dim["CD_ITPRE_MED"].fillna(0)
        df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
        df_dim["CD_TIP_PRESC"] = df_dim["CD_TIP_PRESC"].fillna(0)
        df_dim["CD_TIP_ESQ"] = df_dim["CD_TIP_ESQ"].fillna("0")
        df_dim["CD_FOR_APL"] = df_dim["CD_FOR_APL"].fillna("0")
        df_dim["CD_TIP_FRE"] = df_dim["CD_TIP_FRE"].fillna(0)
        df_dim["TP_JUSTIFICATIVA"] = df_dim["TP_JUSTIFICATIVA"].fillna("0")

        print(df_dim.info())

        df_stage = pd.read_sql(query_itpre_med_hdata.format(cd_pre_med=cd_pre_med), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage["CD_ITPRE_MED"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.ITPRE_MED (CD_PRE_MED, CD_ITPRE_MED, CD_PRODUTO, CD_TIP_PRESC, CD_TIP_ESQ, CD_FOR_APL, CD_TIP_FRE, TP_JUSTIFICATIVA) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"

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

        print("Dados ITPRE_MED inseridos")

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="UPDATE MV_RHP.ITPRE_MED SET CD_PRODUTO = NULL WHERE CD_PRODUTO = 0"

    cursor.execute(sql)

    con.commit()
    cursor.close
    con.close

def df_tip_presc():
    print("Entrou no df_tip_presc")

    df_dim = pd.read_sql(query_tip_presc, connect_rhp())

    print(df_dim)

    df_dim["CD_TIP_PRESC"] = df_dim["CD_TIP_PRESC"].fillna(0)
    df_dim["DS_TIP_PRESC"] = df_dim["DS_TIP_PRESC"].fillna("0")
    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")

    print(df_dim.info())

    df_stage = pd.read_sql(query_tip_presc_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.TIP_PRESC (CD_TIP_PRESC, DS_TIP_PRESC, CD_PRO_FAT) VALUES (:1, :2, :3)"

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

    print("Dados TIP_PRESC inseridos")

def df_for_apl():
    print("Entrou no df_for_apl")

    df_dim = pd.read_sql(query_for_apl, connect_rhp())

    print(df_dim)

    df_dim["CD_FOR_APL"] = df_dim["CD_FOR_APL"].fillna(0)
    df_dim["DS_FOR_APL"] = df_dim["DS_FOR_APL"].fillna("0")

    print(df_dim.info())

    df_stage = pd.read_sql(query_for_apl_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_FOR_APL"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.FOR_APL (CD_FOR_APL, DS_FOR_APL) VALUES (:1, :2)"

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

    print("Dados FOR_APL inseridos")

def df_tip_esq():
    print("Entrou no df_tip_esq")

    df_dim = pd.read_sql(query_tip_esq, connect_rhp())

    print(df_dim)

    df_dim["CD_TIP_ESQ"] = df_dim["CD_TIP_ESQ"].fillna("0")
    df_dim["DS_TIP_ESQ"] = df_dim["DS_TIP_ESQ"].fillna("0")

    print(df_dim.info())

    df_stage = pd.read_sql(query_tip_esq_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_TIP_ESQ"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.TIP_ESQ (CD_TIP_ESQ, DS_TIP_ESQ) VALUES (:1, :2)"

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

    print("Dados TIP_ESQ inseridos")

def df_tip_fre():
    print("Entrou no df_tip_fre")

    df_dim = pd.read_sql(query_tip_fre, connect_rhp())

    print(df_dim)

    df_dim["CD_TIP_FRE"] = df_dim["CD_TIP_FRE"].fillna(0)
    df_dim["DS_TIP_FRE"] = df_dim["DS_TIP_FRE"].fillna("0")

    print(df_dim.info())

    df_stage = pd.read_sql(query_tip_fre_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_TIP_FRE"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.TIP_FRE (CD_TIP_FRE, DS_TIP_FRE) VALUES (:1, :2)"

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

    print(df_dim.info())

    df_stage = pd.read_sql(query_gru_fat_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_GRU_FAT"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.GRU_FAT (CD_GRU_FAT, DS_GRU_FAT, TP_GRU_FAT) VALUES (:1, :2, :3)"

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

    print(df_dim.info())

    df_stage = pd.read_sql(query_gru_pro_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_GRU_PRO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.GRU_PRO (CD_GRU_PRO, CD_GRU_FAT, DS_GRU_PRO, TP_GRU_PRO, TP_CUSTO) VALUES (:1, :2, :3, :4, :5)"

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

    print(df_dim.info())

    df_stage = pd.read_sql(query_produto_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_PRODUTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.PRODUTO (CD_PRO_FAT, DS_PRODUTO, CD_PRODUTO, VL_FATOR_PRO_FAT, SN_OPME, CD_ESPECIE, VL_CUSTO_MEDIO) VALUES (:1, :2, :3, :4, :5, :6, :7)"

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

    print("Dados PRODUTO inseridos")

def df_pro_fat():
    print("Entrou no df_pro_fat")

    df_dim = pd.read_sql(query_pro_fat, connect_rhp())

    print(df_dim)

    df_dim["CD_GRU_PRO"] = df_dim["CD_GRU_PRO"].fillna(0)
    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    df_dim["CD_POR_ANE"] = df_dim["CD_POR_ANE"].fillna(0)
    df_dim["DS_PRO_FAT"] = df_dim["DS_PRO_FAT"].fillna("0")

    print(df_dim.info())

    df_stage = pd.read_sql(query_pro_fat_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_PRO_FAT"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.PRO_FAT (CD_GRU_PRO, CD_PRO_FAT, CD_POR_ANE, DS_PRO_FAT) VALUES (:1, :2, :3, :4)"

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

    print("Dados PRO_FAT inseridos")

def df_tuss():
    print("Entrou no df_tuss")

    df_dim = pd.read_sql(query_tuss, connect_rhp())

    print(df_dim)

    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    df_dim["CD_TUSS"] = df_dim["CD_TUSS"].fillna("0")
    df_dim["DS_TUSS"] = df_dim["DS_TUSS"].fillna("0")

    print(df_dim.info())

    df_stage = pd.read_sql(query_tuss_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_TUSS"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.TUSS (CD_TUSS, CD_PRO_FAT, DS_TUSS) VALUES (:1, :2, :3)"

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

    print(df_dim.info())

    df_stage = pd.read_sql(query_uni_pro_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_UNI_PRO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.UNI_PRO (DS_UNIDADE, CD_UNIDADE, VL_FATOR, TP_RELATORIOS, CD_UNI_PRO, CD_PRODUTO, SN_ATIVO) VALUES (:1, :2, :3, :4, :5, :6, :7)"

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

    print("Dados UNI_PRO inseridos")

def df_reg_amb():
    print("Entrou no df_reg_amb")

    df_dim = pd.read_sql(query_reg_amb, connect_rhp())

    print(df_dim)

    df_dim["CD_REG_AMB"] = df_dim["CD_REG_AMB"].fillna(0)
    df_dim["CD_REMESSA"] = df_dim["CD_REMESSA"].fillna(0)
    df_dim["VL_TOTAL_CONTA"] = df_dim["VL_TOTAL_CONTA"].fillna(0)

    print(df_dim.info())

    df_stage = pd.read_sql(query_reg_amb_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_REG_AMB"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.REG_AMB (CD_REG_AMB, CD_REMESSA, VL_TOTAL_CONTA) VALUES (:1, :2, :3)"

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

    print("Dados REG_AMB inseridos")

def df_itreg_amb():
    print("Entrou no df_itreg_amb")
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2021, 4, 1), until=datetime.datetime(2021, 4, 30)):

        if dt.month == 12:
            data_fim = datetime.datetime(dt.year + 1, 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year + 1, 1, 1)
        else:
            data_fim = datetime.datetime(dt.year, dt.month + 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year, dt.month + 1, 1)

        print(dt.strftime('%d/%m/%Y'), ' a ', data_fim.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_itreg_amb.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp())

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
        df_dim["CD_ITMVTO"] = df_dim["CD_ITMVTO"].fillna(0)

        print(df_dim.info())

        df_stage = pd.read_sql(query_itreg_amb_hdata.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.ITREG_AMB (CD_ATENDIMENTO, CD_PRO_FAT, CD_REG_AMB, CD_GRU_FAT, CD_LANCAMENTO, QT_LANCAMENTO, VL_UNITARIO, VL_NOTA, CD_SETOR, CD_SETOR_PRODUZIU, TP_PAGAMENTO, SN_PERTENCE_PACOTE, VL_TOTAL_CONTA, SN_FECHADA, DT_FECHAMENTO, CD_ITMVTO) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)"

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

        print("Dados ITREG_AMB inseridos")

def df_reg_fat():
    print("Entrou no df_reg_fat")
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2021, 4, 1), until=datetime.datetime(2021, 4, 30)):

        if dt.month == 12:
            data_fim = datetime.datetime(dt.year + 1, 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year + 1, 1, 1)
        else:
            data_fim = datetime.datetime(dt.year, dt.month + 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year, dt.month + 1, 1)

        print(dt.strftime('%d/%m/%Y'), ' a ', data_fim.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_reg_fat.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp())

        print(df_dim)

        df_dim["CD_REG_FAT"] = df_dim["CD_REG_FAT"].fillna(0)
        df_dim["SN_FECHADA"] = df_dim["SN_FECHADA"].fillna("0")
        df_dim["CD_REMESSA"] = df_dim["CD_REMESSA"].fillna(0)
        df_dim["VL_TOTAL_CONTA"] = df_dim["VL_TOTAL_CONTA"].fillna(0)
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)

        print(df_dim.info())

        df_stage = pd.read_sql(query_reg_fat_hdata.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage["CD_REG_FAT"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.REG_FAT (CD_REG_FAT, SN_FECHADA, DT_INICIO, DT_FINAL, DT_FECHAMENTO, CD_REMESSA, VL_TOTAL_CONTA, CD_ATENDIMENTO) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"

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

        print("Dados REG_FAT inseridos")

def df_itreg_fat():
    print("Entrou no df_itreg_fat")
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2021, 4, 1), until=datetime.datetime(2021, 4, 30)):

        if dt.month == 12:
            data_fim = datetime.datetime(dt.year + 1, 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year + 1, 1, 1)
        else:
            data_fim = datetime.datetime(dt.year, dt.month + 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year, dt.month + 1, 1)

        print(dt.strftime('%d/%m/%Y'), ' a ', data_fim.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_itreg_fat.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp())

        print(df_dim)

        df_dim["CD_REG_FAT"] = df_dim["CD_REG_FAT"].fillna(0)
        df_dim["CD_LANCAMENTO"] = df_dim["CD_LANCAMENTO"].fillna(0)
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

        print(df_dim.info())

        df_stage = pd.read_sql(query_itreg_fat_hdata.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.ITREG_FAT (CD_REG_FAT, CD_LANCAMENTO, DT_LANCAMENTO, QT_LANCAMENTO, TP_PAGAMENTO, VL_UNITARIO, VL_NOTA, CD_CONTA_PAI, CD_PRO_FAT, CD_PRO_FAT, CD_GRU_FAT, VL_TOTAL_CONTA, SN_PERTENCE_PACOTE, CD_SETOR, CD_SETOR_PRODUZIU, CD_ITMVTO) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)"

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

        print("Dados ITREG_FAT inseridos")

def df_custo_final():
    print("Entrou no df_custo_final")
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2021, 4, 1), until=datetime.datetime(2021, 4, 30)):

        if dt.month == 12:
            data_fim = datetime.datetime(dt.year + 1, 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year + 1, 1, 1)
        else:
            data_fim = datetime.datetime(dt.year, dt.month + 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year, dt.month + 1, 1)

        print(dt.strftime('%d/%m/%Y'), ' a ', data_fim.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_custo_final.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp())

        print(df_dim)

        df_dim["VL_CUSTO_CENCIR"] = df_dim["VL_CUSTO_CENCIR"].fillna(0)

        print(df_dim.info())

        df_stage = pd.read_sql(query_custo_final_hdata.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.CUSTO_FINAL (VL_CUSTO_CENCIR, DT_COMPETENCIA) VALUES (:1, :2)"

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

        print("Dados CUSTO_FINAL inseridos")

def df_mvto_estoque():
    print("Entrou no df_mvto_estoque")
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2021, 4, 1), until=datetime.datetime(2021, 4, 30)):

        if dt.month == 12:
            data_fim = datetime.datetime(dt.year + 1, 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year + 1, 1, 1)
        else:
            data_fim = datetime.datetime(dt.year, dt.month + 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year, dt.month + 1, 1)

        print(dt.strftime('%d/%m/%Y'), ' a ', data_fim.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_mvto_estoque.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp())

        print(df_dim)

        df_dim["CD_MVTO_ESTOQUE"] = df_dim["CD_MVTO_ESTOQUE"].fillna(0)
        df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
        df_dim["CD_MOT_DEV"] = df_dim["CD_MOT_DEV"].fillna(0)
        df_dim["CD_MULTI_EMPRESA"] = df_dim["CD_MULTI_EMPRESA"].fillna(0)

        lista_cds_mvto_estoque = df_dim['CD_MVTO_ESTOQUE'].to_list()
        lista_cds_mvto_estoque = [str(cd) for cd in lista_cds_mvto_estoque]
        cd_mvto_estoque = ','.join(lista_cds_mvto_estoque)

        print(df_dim.info())

        df_stage = pd.read_sql(query_mvto_estoque_hdata.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage["CD_MVTO_ESTOQUE"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.MVTO_ESTOQUE (CD_MVTO_ESTOQUE, CD_SETOR, CD_ATENDIMENTO, CD_MOT_DEV, CD_MULTI_EMPRESA, DT_MVTO_ESTOQUE) VALUES (:1, :2, :3, :4, :5, :6)"

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

        print("Dados MVTO_ESTOQUE inseridos")

        df_itmvto_estoque(cd_mvto_estoque)

def df_itmvto_estoque(cd_mvto_estoque):
    print("Entrou no df_itmvto_estoque")

    df_dim = pd.read_sql(query_itmvto_estoque.format(cd_mvto_estoque=cd_mvto_estoque), connect_rhp())

    print(df_dim)

    df_dim["CD_ITMVTO_ESTOQUE"] = df_dim["CD_ITMVTO_ESTOQUE"].fillna(0)
    df_dim["QT_MOVIMENTACAO"] = df_dim["QT_MOVIMENTACAO"].fillna(0)
    df_dim["CD_MVTO_ESTOQUE"] = df_dim["CD_MVTO_ESTOQUE"].fillna(0)
    df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
    df_dim["CD_UNI_PRO"] = df_dim["CD_UNI_PRO"].fillna(0)

    print(df_dim.info())

    df_stage = pd.read_sql(query_itmvto_estoque_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_ITMVTO_ESTOQUE"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ITMVTO_ESTOQUE (CD_ITMVTO_ESTOQUE, QT_MOVIMENTACAO, CD_MVTO_ESTOQUE, CD_PRODUTO, CD_UNI_PRO) VALUES (:1, :2, :3, :4, :5)"

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

    print("Dados ITMVTO_ESTOQUE inseridos")

def df_quantidade_diarias():
    print("Entrou no df_quantidade_diarias")

    df_dim = pd.read_sql(query_quantidade_diarias, connect_rhp())

    print(df_dim)

    df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
    df_dim["VL_DIARIA"] = df_dim["VL_DIARIA"].fillna(0)
    df_dim["QTD_DIARIAS"] = df_dim["QTD_DIARIAS"].fillna(0)

    print(df_dim.info())

    df_stage = pd.read_sql(query_quantidade_diarias_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_CID"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.QUANTIDADE_DIARIAS (CD_ATENDIMENTO, VL_DIARIA, QTD_DIARIAS) VALUES (:1, :2, :3)"

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

    print("Dados QUANTIDADE_DIARIAS inseridos")

def df_remessa_fatura():
    print("Entrou no df_remessa_fatura")
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2021, 4, 1), until=datetime.datetime(2021, 4, 30)):

        if dt.month == 12:
            data_fim = datetime.datetime(dt.year + 1, 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year + 1, 1, 1)
        else:
            data_fim = datetime.datetime(dt.year, dt.month + 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year, dt.month + 1, 1)

        print(dt.strftime('%d/%m/%Y'), ' a ', data_fim.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_remessa_fatura.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp())

        print(df_dim)

        df_dim["CD_REMESSA"] = df_dim["CD_REMESSA"].fillna(0)

        print(df_dim.info())

        df_stage = pd.read_sql(query_remessa_fatura_hdata.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage["CD_REMESSA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.REMESSA_FATURA (CD_REMESSA, DT_ABERTURA, DT_FECHAMENTO, DT_ENTREGA_DA_FATURA) VALUES (:1, :2, :3, :4)"

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

        print("Dados REMESSA_FATURA inseridos")

def df_repasse():
    print("Entrou no df_repasse")
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2021, 4, 1), until=datetime.datetime(2021, 4, 30)):

        if dt.month == 12:
            data_fim = datetime.datetime(dt.year + 1, 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year + 1, 1, 1)
        else:
            data_fim = datetime.datetime(dt.year, dt.month + 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year, dt.month + 1, 1)

        print(dt.strftime('%d/%m/%Y'), ' a ', data_fim.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_repasse.format(data_ini=dat_1.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp())

        print(df_dim)

        df_dim["CD_REPASSE"] = df_dim["CD_REPASSE"].fillna(0)

        print(df_dim.info())

        df_stage = pd.read_sql(query_repasse_hdata.format(data_ini=dat_1.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage["CD_REPASSE"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.REPASSE (CD_REPASSE, DT_COMPETENCIA) VALUES (:1, :2)"

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

        print("Dados REPASSE inseridos")

def df_it_repasse():
    print("Entrou no df_it_repasse")

    df_dim = pd.read_sql(query_it_repasse, connect_rhp())

    print(df_dim)

    df_dim["CD_REG_FAT"] = df_dim["CD_REG_FAT"].fillna(0)
    df_dim["CD_LANCAMENTO_FAT"] = df_dim["CD_LANCAMENTO_FAT"].fillna(0)
    df_dim["CD_REPASSE"] = df_dim["CD_REPASSE"].fillna(0)

    print(df_dim.info())

    df_stage = pd.read_sql(query_it_repasse_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.IT_REPASSE (CD_REG_FAT, CD_LANCAMENTO_FAT, CD_REPASSE) VALUES (:1, :2, :3)"

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

    print("Dados IT_REPASSE inseridos")

def df_itent_pro():
    print("Entrou no df_itent_pro")
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2021, 4, 1), until=datetime.datetime(2021, 4, 30)):

        if dt.month == 12:
            data_fim = datetime.datetime(dt.year + 1, 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year + 1, 1, 1)
        else:
            data_fim = datetime.datetime(dt.year, dt.month + 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year, dt.month + 1, 1)

        print(dt.strftime('%d/%m/%Y'), ' a ', data_fim.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_itent_pro.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp())

        print(df_dim)

        df_dim["VL_TOTAL"] = df_dim["VL_TOTAL"].fillna(0)
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
        df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
        df_dim["VL_UNITARIO"] = df_dim["VL_UNITARIO"].fillna(0)
        # df_dim["DT_GRAVACAO"] = df_dim["DT_GRAVACAO"].fillna("01.01.1899 00:00:00")

        print(df_dim.info())

        df_stage = pd.read_sql(query_itent_pro_hdata.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.ITENT_PRO (VL_TOTAL, CD_ATENDIMENTO, CD_PRODUTO, VL_UNITARIO, DT_GRAVACAO) VALUES (:1, :2, :3, :4, :5)"

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

    print(df_dim.info())

    df_stage = pd.read_sql(query_glosas_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_GLOSAS"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.GLOSAS (CD_GLOSAS, CD_REG_FAT, CD_REG_AMB, CD_MOTIVO_GLOSA, VL_GLOSA, CD_LANCAMENTO_FAT, CD_LANCAMENTO_AMB) VALUES (:1, :2, :3, :4, :5, :6, :7)"

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

    print("Dados GLOSAS inseridos")

def df_custo_medio_mensal():
    print("Entrou no df_custo_medio_mensal")
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2021, 4, 1), until=datetime.datetime(2021, 4, 30)):

        if dt.month == 12:
            data_fim = datetime.datetime(dt.year + 1, 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year + 1, 1, 1)
        else:
            data_fim = datetime.datetime(dt.year, dt.month + 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year, dt.month + 1, 1)

        print(dt.strftime('%d/%m/%Y'), ' a ', data_fim.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_custo_medio_mensal.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp())

        print(df_dim)

        df_dim["VL_CUSTO_MEDIO"] = df_dim["VL_CUSTO_MEDIO"].fillna(0)
        # df_dim["DH_CUSTO_MEDIO"] = df_dim["DH_CUSTO_MEDIO"].fillna("01.01.1899 00:00:00")
        df_dim["CD_PRODUTO"] = df_dim["CD_PRODUTO"].fillna(0)
        df_dim["CD_MULTI_EMPRESA"] = df_dim["CD_MULTI_EMPRESA"].fillna(0)

        print(df_dim.info())

        df_stage = pd.read_sql(query_custo_medio_mensal_hdata.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage,indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.CUSTO_MEDIO_MENSAL (VL_CUSTO_MEDIO, DH_CUSTO_MEDIO, CD_PRODUTO, CD_MULTI_EMPRESA) VALUES (:1, :2, :3, :4)"

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

    print(df_dim.info())

    df_stage = pd.read_sql(query_fa_custo_atendimento_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_ATENDIMENTO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.FA_CUSTO_ATENDIMENTO (VL_DIARIA, VL_CUSTO_GASES, VL_CUSTO_REPASSE, VL_CUSTO_MEDICAMENTO, VL_PROCEDIMENTO, VL_CUSTO_DIARIATAXA, CD_ATENDIMENTO) VALUES (:1, :2, :3, :4, :5, :6, :7)"

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

    print("Dados FA_CUSTO_ATENDIMENTO inseridos")

def df_especie():
    print("Entrou no df_especie")

    df_dim = pd.read_sql(query_especie, connect_rhp())

    print(df_dim)

    df_dim["CD_ESPECIE"] = df_dim["CD_ESPECIE"].fillna(0)
    df_dim["DS_ESPECIE"] = df_dim["DS_ESPECIE"].fillna("0")

    print(df_dim.info())

    df_stage = pd.read_sql(query_especie_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_ESPECIE"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.ESPECIE (CD_ESPECIE, DS_ESPECIE) VALUES (:1, :2)"

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

    print("Dados ESPECIE inseridos")

def df_exa_lab():
    print("Entrou no df_exa_lab")

    df_dim = pd.read_sql(query_exa_lab, connect_rhp())

    print(df_dim)

    df_dim["CD_PRO_FAT"] = df_dim["CD_PRO_FAT"].fillna("0")
    df_dim["CD_EXA_LAB"] = df_dim["CD_EXA_LAB"].fillna(0)
    df_dim["NM_EXA_LAB"] = df_dim["NM_EXA_LAB"].fillna("0")

    print(df_dim.info())

    df_stage = pd.read_sql(query_exa_lab_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_EXA_LAB"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.EXA_LAB (CD_PRO_FAT, CD_EXA_LAB, NM_EXA_LAB) VALUES (:1, :2, :3)"

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

    print("Dados EXA_LAB inseridos")

def df_exa_rx():
    print("Entrou no df_exa_rx")

    df_dim = pd.read_sql(query_exa_rx_2, connect_rhp())

    print(df_dim)

    df_dim["EXA_RX_CD_PRO_FAT"] = df_dim["EXA_RX_CD_PRO_FAT"].fillna("0")
    df_dim["CD_EXA_RX"] = df_dim["CD_EXA_RX"].fillna(0)
    df_dim["DS_EXA_RX"] = df_dim["DS_EXA_RX"].fillna("0")
    df_dim["CD_MODALIDADE_EXAME"] = df_dim["CD_MODALIDADE_EXAME"].fillna(0)

    print(df_dim.info())

    df_stage = pd.read_sql(query_exa_rx_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_EXA_RX"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.EXA_RX (EXA_RX_CD_PRO_FAT, CD_EXA_RX, DS_EXA_RX, CD_MODALIDADE_EXAME) VALUES (:1, :2, :3, :4)"

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

    print("Dados EXA_RX inseridos")

def df_motivo_glosa():
    print("Entrou no df_motivo_glosa")

    df_dim = pd.read_sql(query_motivo_glosa, connect_rhp())

    print(df_dim)

    df_dim["DS_MOTIVO_GLOSA"] = df_dim["DS_MOTIVO_GLOSA"].fillna("0")
    df_dim["CD_MOTIVO_GLOSA"] = df_dim["CD_MOTIVO_GLOSA"].fillna(0)

    print(df_dim.info())

    df_stage = pd.read_sql(query_motivo_glosa_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_MOTIVO_GLOSA"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.MOTIVO_GLOSA (DS_MOTIVO_GLOSA, CD_MOTIVO_GLOSA) VALUES (:1, :2)"

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

    print("Dados MOTIVO_GLOSA inseridos")

def df_mot_dev():
    print("Entrou no df_mot_dev")

    df_dim = pd.read_sql(query_mot_dev, connect_rhp())

    print(df_dim)

    df_dim["CD_MOT_DEV"] = df_dim["CD_MOT_DEV"].fillna(0)
    df_dim["DS_MOT_DEV"] = df_dim["DS_MOT_DEV"].fillna("0")

    print(df_dim.info())

    df_stage = pd.read_sql(query_mot_dev_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_MOT_DEV"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.MOT_DEV (CD_MOT_DEV, DS_MOT_DEV) VALUES (:1, :2)"

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

    print("Dados MOT_DEV inseridos")

def df_ped_lab():
    print("Entrou no df_ped_lab")
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2021, 4, 1), until=datetime.datetime(2021, 4, 30)):

        if dt.month == 12:
            data_fim = datetime.datetime(dt.year + 1, 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year + 1, 1, 1)
        else:
            data_fim = datetime.datetime(dt.year, dt.month + 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year, dt.month + 1, 1)

        print(dt.strftime('%d/%m/%Y'), ' a ', data_fim.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_ped_lab.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp())

        df_dim["CD_PED_LAB"] = df_dim["CD_PED_LAB"].fillna(0)
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
        df_dim["CD_PRESTADOR"] = df_dim["CD_PRESTADOR"].fillna(0)
        df_dim["NM_USUARIO"] = df_dim["NM_USUARIO"].fillna("0")
        df_dim["CD_TECNICO_EXA"] = df_dim["CD_TECNICO_EXA"].fillna(0)
        df_dim["TP_SOLICITACAO"] = df_dim["TP_SOLICITACAO"].fillna("0")
        df_dim["CD_SET_EXA"] = df_dim["CD_SET_EXA"].fillna(0)
        df_dim["NM_SET_EXA"] = df_dim["NM_SET_EXA"].fillna("0")
        df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)
        df_dim["CD_PRE_MED"] = df_dim["CD_PRE_MED"].fillna(0)

        print(df_dim.info())

        lista_cds_ped_lab = df_dim['CD_PED_LAB'].to_list()
        lista_cds_ped_lab = [str(cd) for cd in lista_cds_ped_lab]

        df_stage = pd.read_sql(query_ped_lab_hdata.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage['CD_PED_LAB'],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)
        
        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.PED_LAB (CD_PED_LAB, DT_PEDIDO, CD_ATENDIMENTO, CD_PRESTADOR, NM_USUARIO, CD_TECNICO_EXA, TP_SOLICITACAO, CD_SET_EXA, NM_SET_EXA, CD_SETOR, CD_PRE_MED) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)"

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

        print("Dados PED_LAB inseridos")

        df_itped_lab(lista_cds_ped_lab)

def df_itped_lab(lista_cds_ped_lab):
    print("Entrou no df_itped_lab")

    lista_cds_ped_lab_dividida = np.array_split(lista_cds_ped_lab, round(len(lista_cds_ped_lab)/900) + 1)

    for cds in lista_cds_ped_lab_dividida:
        cd_ped_lab = ','.join(cds)

        df_dim = pd.read_sql(query_itped_lab.format(cd_ped_lab=cd_ped_lab), connect_rhp())

        df_dim["CD_PED_LAB"] = df_dim["CD_PED_LAB"].fillna(0)
        df_dim["CD_ITPED_LAB"] = df_dim["CD_ITPED_LAB"].fillna(0)
        df_dim["CD_EXA_LAB"] = df_dim["CD_EXA_LAB"].fillna(0)
        df_dim["NM_EXA_LAB"] = df_dim["NM_EXA_LAB"].fillna("0")
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
        df_dim["SN_REALIZADO"] = df_dim["SN_REALIZADO"].fillna("0")
        df_dim["CD_LABORATORIO"] = df_dim["CD_LABORATORIO"].fillna(0)
        df_dim["CD_SET_EXA"] = df_dim["CD_SET_EXA"].fillna(0)
        df_dim["NM_SET_EXA"] = df_dim["NM_SET_EXA"].fillna("0")
        df_dim["CD_MATERIAL"] = df_dim["CD_MATERIAL"].fillna(0)
        df_dim["CD_ITPRE_MED"] = df_dim["CD_ITPRE_MED"].fillna(0)

        print(df_dim.info())

        df_stage = pd.read_sql(query_itped_lab_hdata.format(cd_ped_lab=cd_ped_lab), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage["CD_ITPED_LAB"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.ITPED_lAB (CD_PED_LAB, CD_ITPED_LAB, CD_EXA_LAB, NM_EXA_LAB, CD_ATENDIMENTO, SN_REALIZADO, CD_LABORATORIO, CD_SET_EXA, NM_SET_EXA, CD_MATERIAL, CD_ITPRE_MED) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)"

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

        print("Dados ITPED_lAB inseridos")

def df_ped_rx():
    print("Entrou no df_ped_rx")
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=datetime.datetime(2021, 4, 1), until=datetime.datetime(2021, 4, 30)):

        if dt.month == 12:
            data_fim = datetime.datetime(dt.year + 1, 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year + 1, 1, 1)
        else:
            data_fim = datetime.datetime(dt.year, dt.month + 1, 1) - datetime.timedelta(1)
            first_day_next_month = datetime.datetime(dt.year, dt.month + 1, 1)

        print(dt.strftime('%d/%m/%Y'), ' a ', data_fim.strftime('%d/%m/%Y'))

        df_dim = pd.read_sql(query_ped_rx.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp())

        df_dim["CD_PED_RX"] = df_dim["CD_PED_RX"].fillna(0)
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
        df_dim["CD_PRESTADOR"] = df_dim["CD_PRESTADOR"].fillna(0)
        df_dim["NM_USUARIO"] = df_dim["NM_USUARIO"].fillna("0")
        df_dim["CD_SET_EXA"] = df_dim["CD_SET_EXA"].fillna(0)
        df_dim["NM_SET_EXA"] = df_dim["NM_SET_EXA"].fillna("0")
        df_dim["CD_SETOR"] = df_dim["CD_SETOR"].fillna(0)
        df_dim["CD_PRE_MED"] = df_dim["CD_PRE_MED"].fillna(0)

        print(df_dim.info())

        lista_cds_ped_rx = df_dim['CD_PED_RX'].to_list()
        lista_cds_ped_rx = [str(cd) for cd in lista_cds_ped_rx]

        df_stage = pd.read_sql(query_ped_rx_hdata.format(data_ini=dt.strftime('%d/%m/%Y'), data_fim=data_fim.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage['CD_PED_RX'],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)
        
        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.PED_RX (CD_PED_RX, DT_PEDIDO, CD_ATENDIMENTO, CD_PRESTADOR, NM_USUARIO, CD_SET_EXA, NM_SET_EXA, CD_SETOR, CD_PRE_MED) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)"

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

        print("Dados PED_RX inseridos")

        df_itped_rx(lista_cds_ped_rx)

def df_itped_rx(lista_cds_ped_rx):
    print("Entrou no df_itped_rx")

    lista_cds_ped_rx_dividida = np.array_split(lista_cds_ped_rx, round(len(lista_cds_ped_rx)/900) + 1)

    for cds in lista_cds_ped_rx_dividida:
        cd_ped_rx = ','.join(cds)

        df_dim = pd.read_sql(query_itped_rx.format(cd_ped_rx=cd_ped_rx), connect_rhp())

        df_dim["CD_PED_RX"] = df_dim["CD_PED_RX"].fillna(0)
        df_dim["CD_ITPED_RX"] = df_dim["CD_ITPED_RX"].fillna(0)
        df_dim["CD_EXA_RX"] = df_dim["CD_EXA_RX"].fillna(0)
        df_dim["CD_LAUDO"] = df_dim["CD_LAUDO"].fillna(0)
        df_dim["SN_REALIZADO"] = df_dim["SN_REALIZADO"].fillna("0")
        df_dim["CD_ITPRE_MED"] = df_dim["CD_ITPRE_MED"].fillna(0)
        df_dim["DS_LAUDO"] = df_dim["DS_LAUDO"].fillna("0")

        print(df_dim.info())

        df_stage = pd.read_sql(query_itped_rx_hdata.format(cd_ped_rx=cd_ped_rx), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage["CD_ITPED_RX"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.ITPED_RX (CD_PED_RX, CD_ITPED_RX, CD_EXA_RX, CD_LAUDO, SN_REALIZADO, CD_ITPRE_MED, DS_LAUDO) VALUES (:1, :2, :3, :4, :5, :6, :7)"

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

        print("Dados ITPED_RX inseridos")

def df_material():
    print("Entrou no df_material")

    df_dim = pd.read_sql(query_material, connect_rhp())

    print(df_dim)

    df_dim["CD_MATERIAL"] = df_dim["CD_MATERIAL"].fillna(0)
    df_dim["DS_MATERIAL"] = df_dim["DS_MATERIAL"].fillna("0")

    print(df_dim.info())

    df_stage = pd.read_sql(query_material_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_MATERIAL"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.MATERIAL (CD_MATERIAL, DS_MATERIAL) VALUES (:1, :2)"

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

    print("Dados MATERIAL inseridos")

def df_mod_exame():
    print("Entrou no df_mod_exame")

    df_dim = pd.read_sql(query_mod_exame, connect_rhp())

    print(df_dim)

    df_dim["CD_MODALIDADE_EXAME"] = df_dim["CD_MODALIDADE_EXAME"].fillna(0)
    df_dim["DS_MODALIDADE_EXAME"] = df_dim["DS_MODALIDADE_EXAME"].fillna("0")

    print(df_dim.info())

    df_stage = pd.read_sql(query_mod_exame_hdata, connect_rhp_hdata())

    df_diff = df_dim.merge(df_stage["CD_MODALIDADE_EXAME"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print("dados para incremento")
    print(df_diff.info())

    con = connect_rhp_hdata()

    cursor = con.cursor()

    sql="INSERT INTO MV_RHP.MOD_EXAME (CD_MODALIDADE_EXAME, DS_MODALIDADE_EXAME) VALUES (:1, :2)"

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

    print("Dados CD_MODALIDADE_EXAME inseridos")

dt_ontem = datetime.datetime.today() - datetime.timedelta(days=1)
dt_ini = dt_ontem - datetime.timedelta(days=5)

dag = DAG("insert_dados_rhp_variabilidade_antigo", default_args=default_args, schedule_interval=None)
# dag = DAG("insert_dados_rhp_variabilidade_antigo", default_args=default_args, schedule_interval="0 6,7 * * *")

t25 = PythonOperator(
    task_id="insert_pre_med_rhp",
    python_callable=df_pre_med,
    dag=dag)

# # t26 = PythonOperator(
# #     task_id="insert_itpre_med_rhp",
# #     python_callable=df_itpre_med,
# #     dag=dag)

# # t27 = PythonOperator(
# #     task_id="insert_tip_presc_rhp",
# #     python_callable=df_tip_presc,
# #     dag=dag)

# t28 = PythonOperator(
#     task_id="insert_for_apl_rhp",
#     python_callable=df_for_apl,
#     dag=dag)

# # t29 = PythonOperator(
# #     task_id="insert_tip_esq_rhp",
# #     python_callable=df_tip_esq,
# #     dag=dag)

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

# # t35 = PythonOperator(
# #     task_id="insert_tuss_rhp",
# #     python_callable=df_tuss,
# #     dag=dag)

# # t36 = PythonOperator(
# #     task_id="insert_uni_pro_rhp",
# #     python_callable=df_uni_pro,
# #     dag=dag)

# # t37 = PythonOperator(
# #     task_id="insert_reg_amb_rhp",
# #     python_callable=df_reg_amb,
# #     dag=dag)

# # t38 = PythonOperator(
# #     task_id="insert_itreg_amb_rhp",
# #     python_callable=df_itreg_amb,
# #     dag=dag)

# # t39 = PythonOperator(
# #     task_id="insert_reg_fat_rhp",
# #     python_callable=df_reg_fat,
# #     dag=dag)

# # t40 = PythonOperator(
# #     task_id="insert_itreg_fat_rhp",
# #     python_callable=df_itreg_fat,
# #     dag=dag)

# # t41 = PythonOperator(
# #     task_id="insert_custo_final_rhp",
# #     python_callable=df_custo_final,
# #     dag=dag)

# # t42 = PythonOperator(
# #     task_id="insert_mvto_estoque_rhp",
# #     python_callable=df_mvto_estoque,
# #     dag=dag)

# # t43 = PythonOperator(
# #     task_id="insert_itmvto_estoque_rhp",
# #     python_callable=df_itmvto_estoque,
# #     dag=dag)

# # t44 = PythonOperator(
# #     task_id="insert_quantidade_diarias_rhp",
# #     python_callable=df_quantidade_diarias,
# #     dag=dag)

# t45 = PythonOperator(
#     task_id="insert_remessa_fatura_rhp",
#     python_callable=df_remessa_fatura,
#     dag=dag)

# # t46 = PythonOperator(
# #     task_id="insert_repasse_rhp",
# #     python_callable=df_repasse,
# #     dag=dag)

# # t47 = PythonOperator(
# #     task_id="insert_it_repasse_rhp",
# #     python_callable=df_it_repasse,
# #     dag=dag)

# # t48 = PythonOperator(
# #     task_id="insert_itent_pro_rhp",
# #     python_callable=df_itent_pro,
# #     dag=dag)

# # t49 = PythonOperator(
# #     task_id="insert_glosas_rhp",
# #     python_callable=df_glosas,
# #     dag=dag)

# # t50 = PythonOperator(
# #     task_id="insert_custo_medio_mensal_rhp",
# #     python_callable=df_custo_medio_mensal,
# #     dag=dag)

# # t51 = PythonOperator(
# #     task_id="insert_fa_custo_atendimento_rhp",
# #     python_callable=df_fa_custo_atendimento,
# #     dag=dag)

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

# # t56 = PythonOperator(
# #     task_id="insert_motivo_glosa_rhp",
# #     python_callable=df_motivo_glosa,
# #     dag=dag)

# # t57 = PythonOperator(
# #     task_id="insert_mot_dev_rhp",
# #     python_callable=df_mot_dev,
# #     dag=dag)
    
t58 = PythonOperator(
    task_id="insert_material_rhp",
    python_callable=df_material,
    dag=dag)
    
t59 = PythonOperator(
    task_id="insert_mod_exame_rhp",
    python_callable=df_mod_exame,
    dag=dag)

t60 = PythonOperator(
    task_id="insert_ped_lab_rhp",
    python_callable=df_ped_lab,
    dag=dag)
    
t61 = PythonOperator(
    task_id="insert_ped_rx_rhp",
    python_callable=df_ped_rx,
    dag=dag)

t25 >> t58 >> t59 >> t60 >> t61