import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from connections.oracle.connections import connect_rhp, connect_rhp_2
from collections import OrderedDict as od
import unidecode

from queries.rhp.queries import *

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

    df = pd.read_sql(query_atendime, connect_rhp_2())

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

def df_pre_med():
    print("Entrou no df_pre_med")

    df = pd.read_sql(query_pre_med, connect_rhp())

    print(df)

def df_itpre_med():
    print("Entrou no df_itpre_med")

    df = pd.read_sql(query_itpre_med, connect_rhp())

    print(df)

def df_tip_presc():
    print("Entrou no df_tip_presc")

    df = pd.read_sql(query_tip_presc, connect_rhp())

    print(df)

def df_for_apl():
    print("Entrou no df_for_apl")

    df = pd.read_sql(query_for_apl, connect_rhp())

    print(df)

def df_tip_esq():
    print("Entrou no df_tip_esq")

    df = pd.read_sql(query_tip_esq, connect_rhp())

    print(df)

def df_tip_fre():
    print("Entrou no df_tip_fre")

    df = pd.read_sql(query_tip_fre, connect_rhp())

    print(df)

def df_de_para_tuss():
    print("Entrou no df_de_para_tuss")

    df = pd.read_sql(query_de_para_tuss, connect_rhp())

    print(df)

def df_gru_fat():
    print("Entrou no df_gru_fat")

    df = pd.read_sql(query_gru_fat, connect_rhp())

    print(df)

def df_gru_pro():
    print("Entrou no df_gru_pro")

    df = pd.read_sql(query_gru_pro, connect_rhp())

    print(df)

def df_produto():
    print("Entrou no df_produto")

    df = pd.read_sql(query_produto, connect_rhp())

    print(df)

def df_pro_fat():
    print("Entrou no df_pro_fat")

    df = pd.read_sql(query_pro_fat, connect_rhp())

    print(df)

def df_tuss():
    print("Entrou no df_tuss")

    df = pd.read_sql(query_tuss, connect_rhp())

    print(df)

def df_uni_pro():
    print("Entrou no df_uni_pro")

    df = pd.read_sql(query_uni_pro, connect_rhp())

    print(df)

def df_reg_amb():
    print("Entrou no df_reg_amb")

    df = pd.read_sql(query_reg_amb, connect_rhp())

    print(df)

def df_itreg_amb():
    print("Entrou no df_itreg_amb")

    df = pd.read_sql(query_itreg_amb, connect_rhp())

    print(df)

def df_reg_fat():
    print("Entrou no df_reg_fat")

    df = pd.read_sql(query_reg_fat, connect_rhp())

    print(df)

def df_itreg_fat():
    print("Entrou no df_itreg_fat")

    df = pd.read_sql(query_itreg_fat, connect_rhp())

    print(df)

def df_custo_final():
    print("Entrou no df_custo_final")

    df = pd.read_sql(query_custo_final, connect_rhp())

    print(df)

def df_mvto_estoque():
    print("Entrou no df_mvto_estoque")

    df = pd.read_sql(query_mvto_estoque, connect_rhp())

    print(df)

def df_itmvto_estoque():
    print("Entrou no df_itmvto_estoque")

    df = pd.read_sql(query_itmvto_estoque, connect_rhp())

    print(df)

def df_quantidade_diarias():
    print("Entrou no df_quantidade_diarias")

    df = pd.read_sql(query_quantidade_diarias, connect_rhp())

    print(df)

def df_remessa_fatura():
    print("Entrou no df_remessa_fatura")

    df = pd.read_sql(query_remessa_fatura, connect_rhp())

    print(df)

def df_repasse():
    print("Entrou no df_repasse")

    df = pd.read_sql(query_repasse, connect_rhp())

    print(df)

def df_it_repasse():
    print("Entrou no df_it_repasse")

    df = pd.read_sql(query_it_repasse, connect_rhp())

    print(df)

def df_itent_pro():
    print("Entrou no df_itent_pro")

    df = pd.read_sql(query_itent_pro, connect_rhp())

    print(df)

def df_glosas():
    print("Entrou no df_glosas")

    df = pd.read_sql(query_glosas, connect_rhp())

    print(df)

def df_custo_medio_mensal():
    print("Entrou no df_custo_medio_mensal")

    df = pd.read_sql(query_custo_medio_mensal, connect_rhp())

    print(df)

def df_fa_custo_atendimento():
    print("Entrou no df_fa_custo_atendimento")

    df = pd.read_sql(query_fa_custo_atendimento, connect_rhp())

    print(df)

def df_especie():
    print("Entrou no df_especie")

    df = pd.read_sql(query_especie, connect_rhp())

    print(df)

def df_exa_lab():
    print("Entrou no df_exa_lab")

    df = pd.read_sql(query_exa_lab, connect_rhp())

    print(df)

def df_exa_rx():
    print("Entrou no df_exa_rx")

    df = pd.read_sql(query_exa_rx, connect_rhp())

    print(df)

def df_gru_fat():
    print("Entrou no df_gru_fat")

    df = pd.read_sql(query_gru_fat, connect_rhp())

    print(df)

def df_motivo_glosa():
    print("Entrou no df_motivo_glosa")

    df = pd.read_sql(query_motivo_glosa, connect_rhp())

    print(df)

def df_mot_dev():
    print("Entrou no df_mot_dev")

    df = pd.read_sql(query_mot_dev, connect_rhp())

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

t25 = PythonOperator(
    task_id="insert_pre_med_rhp",
    python_callable=df_pre_med,
    dag=dag)

t26 = PythonOperator(
    task_id="insert_itpre_med_rhp",
    python_callable=df_itpre_med,
    dag=dag)

t27 = PythonOperator(
    task_id="insert_tip_presc_rhp",
    python_callable=df_tip_presc,
    dag=dag)

t28 = PythonOperator(
    task_id="insert_for_apl_rhp",
    python_callable=df_for_apl,
    dag=dag)

t29 = PythonOperator(
    task_id="insert_tip_esq_rhp",
    python_callable=df_tip_esq,
    dag=dag)

t30 = PythonOperator(
    task_id="insert_tip_fre_rhp",
    python_callable=df_tip_fre,
    dag=dag)

t31 = PythonOperator(
    task_id="insert_de_para_tuss_rhp",
    python_callable=df_de_para_tuss,
    dag=dag)

t32 = PythonOperator(
    task_id="insert_gru_pro_rhp",
    python_callable=df_gru_pro,
    dag=dag)

t33 = PythonOperator(
    task_id="insert_produto_rhp",
    python_callable=df_produto,
    dag=dag)

t34 = PythonOperator(
    task_id="insert_pro_fat_rhp",
    python_callable=df_pro_fat,
    dag=dag)

t35 = PythonOperator(
    task_id="insert_tuss_rhp",
    python_callable=df_tuss,
    dag=dag)

t36 = PythonOperator(
    task_id="insert_uni_pro_rhp",
    python_callable=df_uni_pro,
    dag=dag)

t37 = PythonOperator(
    task_id="insert_reg_amb_rhp",
    python_callable=df_reg_amb,
    dag=dag)

t38 = PythonOperator(
    task_id="insert_itreg_amb_rhp",
    python_callable=df_itreg_amb,
    dag=dag)

t39 = PythonOperator(
    task_id="insert_reg_fat_rhp",
    python_callable=df_reg_fat,
    dag=dag)

t40 = PythonOperator(
    task_id="insert_itreg_fat_rhp",
    python_callable=df_itreg_fat,
    dag=dag)

t41 = PythonOperator(
    task_id="insert_custo_final_rhp",
    python_callable=df_custo_final,
    dag=dag)

t42 = PythonOperator(
    task_id="insert_mvto_estoque_rhp",
    python_callable=df_mvto_estoque,
    dag=dag)

t43 = PythonOperator(
    task_id="insert_itmvto_estoque_rhp",
    python_callable=df_itmvto_estoque,
    dag=dag)

t44 = PythonOperator(
    task_id="insert_quantidade_diarias_rhp",
    python_callable=df_quantidade_diarias,
    dag=dag)

t45 = PythonOperator(
    task_id="insert_remessa_fatura_rhp",
    python_callable=df_remessa_fatura,
    dag=dag)

t46 = PythonOperator(
    task_id="insert_repasse_rhp",
    python_callable=df_repasse,
    dag=dag)

t47 = PythonOperator(
    task_id="insert_it_repasse_rhp",
    python_callable=df_it_repasse,
    dag=dag)

t48 = PythonOperator(
    task_id="insert_itent_pro_rhp",
    python_callable=df_itent_pro,
    dag=dag)

t49 = PythonOperator(
    task_id="insert_glosas_rhp",
    python_callable=df_glosas,
    dag=dag)

t50 = PythonOperator(
    task_id="insert_custo_medio_mensal_rhp",
    python_callable=df_custo_medio_mensal,
    dag=dag)

t51 = PythonOperator(
    task_id="insert_fa_custo_atendimento_rhp",
    python_callable=df_fa_custo_atendimento,
    dag=dag)

t52 = PythonOperator(
    task_id="insert_especie_rhp",
    python_callable=df_especie,
    dag=dag)

t53 = PythonOperator(
    task_id="insert_exa_lab_rhp",
    python_callable=df_exa_lab,
    dag=dag)

t54 = PythonOperator(
    task_id="insert_exa_rx_rhp",
    python_callable=df_exa_rx,
    dag=dag)

t55 = PythonOperator(
    task_id="insert_gru_fat_rhp",
    python_callable=df_gru_fat,
    dag=dag)

t56 = PythonOperator(
    task_id="insert_motivo_glosa_rhp",
    python_callable=df_motivo_glosa,
    dag=dag)

t57 = PythonOperator(
    task_id="insert_mot_dev_rhp",
    python_callable=df_mot_dev,
    dag=dag)

t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11 >> t12 >> t13 >> t14 >> t15 >> t16 >> t17 >> t18 >> t19 >> t20 >> t21 >> t22 >> t23 >> t24 >> t25 >> t26 >> t27 >> t28 >> t29 >> t30 >> t31 >> t32 >> t33 >> t34 >> t35 >> t36 >> t37 >> t38 >> t39 >> t40 >> t41 >> t42 >> t43 >> t44 >> t45 >> t46 >> t47 >> t48 >> t49 >> t50 >> t51 >> t52 >> t53 >> t54 >> t55 >> t56 >> t57