import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from connections.oracle.connections import connect_rhp, connect_rhp_2
from collections import OrderedDict as od
import unidecode

from queries.rhp.atendime import query_atendime
from queries.rhp.cid import query_cid
from queries.rhp.classificacao_risco import query_classificacao_risco
from queries.rhp.classificacao import query_classificacao
from queries.rhp.convenio import query_convenio
from queries.rhp.cor_referencia import query_cor_referencia
from queries.rhp.diagnostico_atendime import query_diagnostico_atendime
from queries.rhp.documento_clinico import query_documento_clinico
from queries.rhp.esp_med import query_esp_med
from queries.rhp.especialidad import query_especialidad
from queries.rhp.gru_cid import query_gru_cid
from queries.rhp.mot_alt import query_mot_alt
from queries.rhp.multi_empresa import query_multi_empresa
from queries.rhp.ori_ate import query_ori_ate
from queries.rhp.paciente import query_paciente
from queries.rhp.pagu_objeto import query_pagu_objeto
from queries.rhp.registro_alta import query_registro_alta
from queries.rhp.setor import query_setor
from queries.rhp.sgru_cid import query_sgru_cid
from queries.rhp.sintoma_avaliacao import query_sintoma_avaliacao
from queries.rhp.tempo_processo import query_tempo_processo
from queries.rhp.tip_mar import query_tip_mar
from queries.rhp.tip_res import query_tip_res
from queries.rhp.triagem_atendimento import query_triagem_atendimento
from queries.rhp.usuario import query_usuario

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

def df_cor_referencia():
    print("Entrou no df_cor_referencia")

    df = pd.read_sql(query_cor_referencia, connect_rhp())

    print(df)

def df_diagnostico_atendime():
    print("Entrou no df_diagnostico_atendime")

    df = pd.read_sql(query_diagnostico_atendime, connect_rhp())

    print(df)

def df_cor_referencia():
    print("Entrou no df_cor_referencia")

    df = pd.read_sql(query_cor_referencia, connect_rhp())

    print(df)

def df_documento_clinico():
    print("Entrou no df_documento_clinico")

    df = pd.read_sql(query_documento_clinico, connect_rhp())

    print(df)

def df_esp_med():
    print("Entrou no df_esp_med")

    df = pd.read_sql(query_esp_med, connect_rhp())

    print(df)

def df_especialidad():
    print("Entrou no df_especialidad")

    df = pd.read_sql(query_especialidad, connect_rhp())

    print(df)

def df_gru_cid():
    print("Entrou no df_gru_cid")

    df = pd.read_sql(query_gru_cid, connect_rhp())

    print(df)

def df_mot_alt():
    print("Entrou no df_mot_alt")

    df = pd.read_sql(query_mot_alt, connect_rhp())

    print(df)

def df_multi_empresa():
    print("Entrou no df_multi_empresa")

    df = pd.read_sql(query_multi_empresa, connect_rhp())

    print(df)

def df_ori_ate():
    print("Entrou no df_ori_ate")

    df = pd.read_sql(query_ori_ate, connect_rhp())

    print(df)

def df_paciente():
    print("Entrou no df_paciente")

    df = pd.read_sql(query_paciente, connect_rhp())

    print(df)

def df_pagu_objeto():
    print("Entrou no df_pagu_objeto")

    df = pd.read_sql(query_pagu_objeto, connect_rhp())

    print(df)

def df_registro_alta():
    print("Entrou no df_registro_alta")

    df = pd.read_sql(query_registro_alta, connect_rhp())

    print(df)

def df_setor():
    print("Entrou no df_setor")

    df = pd.read_sql(query_setor, connect_rhp())

    print(df)

def df_sgru_cid():
    print("Entrou no df_sgru_cid")

    df = pd.read_sql(query_sgru_cid, connect_rhp())

    print(df)

def df_sintoma_avaliacao():
    print("Entrou no df_sintoma_avaliacao")

    df = pd.read_sql(query_sintoma_avaliacao, connect_rhp())

    print(df)

def df_tempo_processo():
    print("Entrou no df_tempo_processo")

    df = pd.read_sql(query_tempo_processo, connect_rhp())

    print(df)

def df_tip_mar():
    print("Entrou no df_tip_mar")

    df = pd.read_sql(query_tip_mar, connect_rhp())

    print(df)

def df_tip_res():
    print("Entrou no df_tip_res")

    df = pd.read_sql(query_tip_res, connect_rhp())

    print(df)

def df_triagem_atendimento():
    print("Entrou no df_triagem_atendimento")

    df = pd.read_sql(query_triagem_atendimento, connect_rhp())

    print(df)

def df_usuario():
    print("Entrou no df_usuario")

    df = pd.read_sql(query_usuario, connect_rhp())

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

t11 = PythonOperator(
    task_id="insert_mot_alt_rhp",
    python_callable=df_mot_alt,
    dag=dag)

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

t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11 >> t12 >> t13 >> t14 >> t15 >> t16 >> t17 >> t18 >> t19 >> t20 >> t21 >> t22 >> t23 >> t24