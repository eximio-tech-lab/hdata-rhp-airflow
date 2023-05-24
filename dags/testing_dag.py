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
from queries.rhp.queries import query_documento_clinico_fec, query_editor_clinico_fec, query_editor_campo
from queries.rhp.queries_hdata import query_documento_clinico_hdata_fec, query_editor_clinico_hdata_fec, query_editor_campo_hdata

START_DATE = airflow.utils.dates.days_ago(2)

default_args = {
    "owner": "Lucas R. Freire",
    "depends_on_past": False,
    "start_date": datetime.datetime(2023,1,24),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=0),
    "provide_context": True,
}

dt_ontem = datetime.datetime.today() - datetime.timedelta(days=1)

def testing(**context):
    error_message("Chamada para campo sucesso RHP",
            ["lucas.freire@hdata.med.br"],
            ["--------",
            "sucesso"],
            type='Stage')
    # df_editor_campo()
    df_editor_clinico()
    print('OK!')

def df_documento_clinico():
    print("Entrou no df_documento_clinico")
    for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.datetime(2022, 5, 30), until=dt_ontem):
    # for dt in rrule.rrule(rrule.DAILY, dtstart=dt_ini, until=dt_ontem):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))

        print(query_documento_clinico_fec.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')))
        df_dim = pd.read_sql(query_documento_clinico_fec.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_rhp())
        print(df_dim.info())

        df_dim["CD_DOCUMENTO_CLINICO"] = df_dim["CD_DOCUMENTO_CLINICO"].fillna(0)
        df_dim["CD_OBJETO"] = df_dim["CD_OBJETO"].fillna(0)
        df_dim["CD_ATENDIMENTO"] = df_dim["CD_ATENDIMENTO"].fillna(0)
        df_dim["CD_TIPO_DOCUMENTO"] = df_dim["CD_TIPO_DOCUMENTO"].fillna(0)
        df_dim["TP_STATUS"] = df_dim["TP_STATUS"].fillna("0")
        df_dim["NM_DOCUMENTO"] = df_dim["NM_DOCUMENTO"].fillna("0")
        df_dim["CD_USUARIO"] = df_dim["CD_USUARIO"].fillna("0")
        df_dim["CD_PRESTADOR"] = df_dim["CD_PRESTADOR"].fillna(0)

        print(query_documento_clinico_hdata_fec.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')))
        df_stage = pd.read_sql(query_documento_clinico_hdata_fec.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_rhp_hdata())
        print(df_stage.info())

        df_diff = df_dim.merge(df_stage["CD_DOCUMENTO_CLINICO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)

        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.PW_DOCUMENTO_CLINICO (CD_DOCUMENTO_CLINICO, CD_OBJETO, CD_ATENDIMENTO, CD_TIPO_DOCUMENTO, TP_STATUS, DH_CRIACAO, DH_FECHAMENTO, NM_DOCUMENTO, CD_USUARIO, CD_PRESTADOR) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)"

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

def df_editor_clinico():
    print("Entrou no df_editor_clinico")
    for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.datetime(2023, 1, 1), until=dt_ontem):
    # for dt in rrule.rrule(rrule.DAILY, dtstart=dt_ini, until=dt_ontem):
        data_1 = dt
        data_2 = dt

        print(data_1.strftime('%d/%m/%Y'), ' a ', data_2.strftime('%d/%m/%Y'))
        try:
            df_dim = pd.read_sql(query_editor_clinico_fec.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_rhp())
        except Exception as e:
            error_message("Erro Carga Rhp",
            ["lucas.freire@hdata.med.br"],
            ["--------",
             "Recuperar dados view editor clinico",
            str(e)],
            type='Stage')
            raise ValueError(e)

        df_dim["CD_EDITOR_CLINICO"] = df_dim["CD_EDITOR_CLINICO"].fillna(999888)
        df_dim["CD_DOCUMENTO_CLINICO"] = df_dim["CD_DOCUMENTO_CLINICO"].fillna(999888)
        df_dim["CD_DOCUMENTO"] = df_dim["CD_DOCUMENTO"].fillna(999888)
        df_dim["CD_EDITOR_REGISTRO"] = df_dim["CD_EDITOR_REGISTRO"].fillna(999888)

        df_stage = pd.read_sql(query_editor_clinico_hdata_fec.format(data_ini=data_1.strftime('%d/%m/%Y'), data_fim=data_2.strftime('%d/%m/%Y')), connect_rhp_hdata())

        df_diff = df_dim.merge(df_stage["CD_EDITOR_CLINICO"],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)
        print("dados para incremento")
        print(df_diff.info())

        con = connect_rhp_hdata()

        cursor = con.cursor()

        sql="INSERT INTO MV_RHP.PW_EDITOR_CLINICO (CD_EDITOR_CLINICO, CD_DOCUMENTO_CLINICO, CD_DOCUMENTO, CD_EDITOR_REGISTRO) VALUES (:1, :2, :3, :4)"

        df_list = df_diff.values.tolist()
        n = 0
        cols = []
        for i in df_diff.iterrows():
            cols.append(df_list[n])
            n += 1
        try:
            cursor.executemany(sql, cols)
        except Exception as e:
            error_message("Erro Carga Rhp",
            ["lucas.freire@hdata.med.br"],
            ["--------",
             "erro de inserção",
            str(e)],
            type='Stage')
            raise ValueError(e)

        con.commit()
        cursor.close
        con.close

        print("Dados PW_EDITOR_CLINICO inseridos")

def update_all_cid():
    print('Ok!')
    # for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.datetime(2022,1,1), until=dt_ontem):
    #     df_dim = pd.read_sql(query_update_all_cid.format(data=dt.strftime('%d/%m/%Y')), connect_rhp())
    #     print("dados para incremento")
    #     print(df_dim.info())

    #     con = connect_rhp_hdata()
    #     cursor = con.cursor()

    #     sql="INSERT INTO MV_RHP.UPDATE_CID_TEMP (CD_ATENDIMENTO, CD_MULTI_EMPRESA, CD_CID) VALUES (:1, :2, :3)"

    #     df_list = df_dim.values.tolist()
    #     n = 0
    #     cols = []
    #     for i in df_dim.iterrows():
    #         cols.append(df_list[n])
    #         n += 1

    #     cursor.executemany(sql, cols)
    #     con.commit()
    #     cursor.close
    #     con.close
    #error_message("CIDs ATUALIZADOS RHP",
    #        ["lucas.freire@hdata.med.br","raphael.queiroz@hdata.med.br"],
    #        ["mensagem vinda do RHP",
    #        "sucesso"],
    #        type='Stage')
    
    print("Dados inseridos")

def df_editor_campo():
    print("Entrou no editor_campo")
    try:
        df_dim = pd.read_sql(query_editor_campo, connect_rhp())
    except Exception as e:
        error_message("Erro Carga Rhp",
            ["lucas.freire@hdata.med.br"],
            ["--------",
             "Recuperar dados view",
            str(e)],
            type='Stage')
        raise ValueError(e)

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
    try:
        cursor.executemany(sql, cols)
    except Exception as e:
        error_message("Erro Carga Rhp",
            ["lucas.freire@hdata.med.br"],
            ["--------",
            "Erro de inserção",
            str(e)],
            type='Stage')
        raise ValueError(e)
    con.commit()
    cursor.close
    con.close

    print("Dados editor_campo inseridos")

dag = DAG("testing_dag", default_args=default_args, schedule_interval="40 13 * * *")

# t0 = PythonOperator(
#     task_id="update_all_pw_doc_clinico",
#     python_callable=df_documento_clinico,
#     dag=dag
# )

t1 = PythonOperator(
    task_id="test",
    python_callable=testing,
    dag=dag
)

t1