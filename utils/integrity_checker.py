import datetime
import os

import emails
import pandas as pd
from airflow.utils.email import send_email

from connections.oracle.connections import connect_sthelena_ssh
from connections.postgres.connections import connect_ssh_rds_postgres
from queries.sthelena.atendimento import query_atendimentos
from utils.google_storage_functions import read_excel, upload_blob, read_csv
from utils.update import df_to_sql_updater


def percorre_colunas(col1, col2):
    error_cols = []
    for nome_coluna in col1:
        if nome_coluna not in col2:
            error_cols.append(nome_coluna)
    return error_cols


def print_log(error_cols):
    if not error_cols:
        print(str(error_cols), '- Não há colunas')
    else:
        [print(error_cols[i]) for i in range(len(error_cols))]


def checa_integridade_colunas(lista_bd_bruto, colunas_padrao):
    colunas_ausentes = []
    colunas_nao_reconhecidas = []
    print(lista_bd_bruto.tolist())

    colunas_ausentes = percorre_colunas(colunas_padrao, lista_bd_bruto)

    colunas_nao_reconhecidas = percorre_colunas(lista_bd_bruto, colunas_padrao)

    print('============= Colunas ausentes: ')
    print_log(colunas_ausentes)

    print('============= Colunas não renhecidas: ')
    print_log(colunas_nao_reconhecidas)

    print('As colunas ausentes serão carregadas vazias no dash e as não reconhecidas serão ignoradas.')
    print('continuando script')
    return colunas_ausentes, colunas_nao_reconhecidas


def correct_dateformat(date_text):
    if date_text is not None:
        try:
            datetime.datetime.strptime(date_text, '%d/%m/%Y %H:%M:%S')
        except ValueError:
            return date_text + ' 00:00:00'
    return date_text


def fill_cols(df_inteiro, colunas, colunas_ausentes):
    df_novo = pd.DataFrame()
    first_flag = True
    for col in colunas:
        if col not in colunas_ausentes:
            if first_flag:
                # print(col,'<---first')
                df_novo = df_novo.join(df_inteiro[col], how='outer')
                first_flag = False
            else:
                # print(col,'<---')
                df_novo = df_novo.join(df_inteiro[col])
        else:
            df_novo[col] = None
    return df_novo


def update_tabelao_database(table_name, primary_key, query=None, tp_entrada='banco_de_dados', file_location_raw=None,
                            expected_columns=None, coluna_paciente=None):
    df_columns = pd.read_sql(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'",
                             connect_ssh_rds_postgres)

    columns_list = df_columns['column_name'].tolist()

    header_counter = -1
    if tp_entrada == 'banco_de_dados':
        df_new_batch = pd.read_sql(query, connect_sthelena_ssh())

        print('COLUNAS NOVAS OU NÃO IDENTIFICADAS:')
        for column in df_new_batch.columns:
            if column.lower() not in columns_list:
                print('->', column)

    elif tp_entrada == 'planilha':
        while True:
            header_counter += 1
            df_new_batch, blob = read_excel(file_location_raw, header_counter)

            if df_new_batch is None:
                return -1

            colunas_ausentes, colunas_nao_reconhecidas = checa_integridade_colunas(df_new_batch.columns,
                                                                                   expected_columns)

            if colunas_ausentes != expected_columns:
                break
        df_new_batch = df_new_batch.drop(columns=colunas_nao_reconhecidas)
        if coluna_paciente:
            df_new_batch = df_new_batch.drop(columns=[coluna_paciente])

    df_raw = pd.read_sql(f'SELECT * FROM RAW.{table_name}', connect_ssh_rds_postgres)

    df_raw.columns = [x.lower() for x in df_raw.columns]
    df_new_batch.columns = [x.lower() for x in df_new_batch.columns]

    # # Seleciona a diferença entre os dois dataframes
    df_diff = df_new_batch.merge(df_raw[primary_key], indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']
    df_diff = df_diff.drop(columns=['_merge'])
    df_diff = df_diff.reset_index(drop=True)

    print('dados para incremento')
    print(df_diff.info())
    df_diff.to_sql(table_name, con=connect_ssh_rds_postgres, if_exists='append', index=False, method='multi',
                   chunksize=10000, schema='raw')

    # Seleciona dados atualizados para fazer o update
    df_eq = df_new_batch.merge(df_raw[primary_key], indicator=True, how='left').loc[lambda x: x['_merge'] == 'both']
    df_eq = df_eq.drop(columns=['_merge'])
    df_eq = df_eq.reset_index(drop=True)
    df_eq.drop_duplicates(subset=[primary_key], inplace=True, keep="last")

    # Tir aspas de strings no stbarbara
    if table_name == 'stbarbara_raw':
        df_eq['queixa_principal_medico'] = df_eq['queixa_principal_medico'].astype(str)
        df_eq['queixa_principal_medico'] = df_eq['queixa_principal_medico'].apply(lambda x: x.replace("'", ""))
        df_eq['queixa_principal_medico'] = df_eq['queixa_principal_medico'].apply(lambda x: x.replace("\"", ""))
        df_eq['queixa_principal_triagem'] = df_eq['queixa_principal_triagem'].astype(str)
        df_eq['queixa_principal_triagem'] = df_eq['queixa_principal_triagem'].apply(lambda x: x.replace("'", ""))
        df_eq['queixa_principal_triagem'] = df_eq['queixa_principal_triagem'].apply(lambda x: x.replace("\"", ""))

    print('dados para atualização')
    print(df_eq.info())
    df_eq = df_eq.astype(object)
    df_eq = df_eq.where((pd.notnull(df_eq)), None)

    lista_sem_cd_atendimento = df_eq.columns.to_list()
    lista_sem_cd_atendimento.remove(primary_key)

    if not df_eq.empty:
        for col in lista_sem_cd_atendimento:
            print(col)
            df_to_sql_updater(df_raw, df_eq, primary_key, 'raw.' + table_name, col, 'dw_eximio')

    return header_counter


def update_tabelao(file_location_raw, file_location_refined, cols_pattern, primary_key, nm_paciente,
                   optional_col_fix_function=None, pk_fix=False, source_type='planilha'):
    header_counter = -1
    if source_type == 'planilha':
        while True:
            header_counter += 1
            df_raw, blob = read_excel(file_location_raw, header_counter)

            if df_raw is None:
                return -1

            if optional_col_fix_function:
                df_raw = optional_col_fix_function(df_raw)

            colunas_ausentes, colunas_nao_reconhecidas = checa_integridade_colunas(df_raw.columns, cols_pattern)

            if colunas_ausentes != cols_pattern:
                print('DEU O BREAK !!!')
                break

        # Cria novo dataframe onde tera o dataframe no formato padrão
        df_clean = pd.DataFrame()
        for col_name in cols_pattern:
            if col_name not in colunas_ausentes:
                if col_name == cols_pattern[0]:
                    # Primeiro join deve ser 'outer' pois o axis está vazio
                    df_clean = df_clean.join(df_raw[col_name], how='outer')
                else:
                    df_clean = df_clean.join(df_raw[col_name])
            # Se tiver uma coluna ausente, preenche ela no lugar certo com None's
            else:
                df_clean[col_name] = None

        if pk_fix:
            df_clean[primary_key] = df_clean.apply(lambda x: int(str(x[primary_key]) + '0' + str(x['PRONTUARIO'])),
                                                   axis=1)

        if df_clean.dropna(how='all').empty:
            raise ValueError('DataFrame vazio!')
    elif source_type == 'banco_de_dados':
        df_clean = pd.read_sql(query_atendimentos, connect_sthelena_ssh())

    # Pega o df do refined
    df_refined, blob = read_csv(file_location_refined)

    print(df_refined.info())

    # Concatena arquivo atual com o tabelao excel atual
    df_refined = pd.concat([df_refined, df_clean])
    df_refined = df_refined.reset_index(drop=True)
    # Ordena e exclui repetidos pelo cd_atendimento
    df_refined = df_refined.sort_values(by=primary_key)
    df_refined = df_refined.reset_index(drop=True)
    df_refined = df_refined.drop_duplicates(subset=primary_key, keep='last')
    if nm_paciente and source_type == 'planilha':
        df_refined[nm_paciente] = None

    print(df_refined.info())

    upload_blob(file_location_refined, df_refined)

    return header_counter


def notify_email(contextDict, **kwargs):
    # Prepare the email
    message = emails.html(
        html="""
            Olá consagrado! <br>
            <br>
            Houve um erro na task {}.<br>
            <br>
            Descrição do erro: {}. <br>
        
            Até logo,<br>
            Airflow. <br>
            """.format(contextDict['task_instance_key_str'], contextDict['exception']),
        subject="Airflow alert: {} Failed".format(contextDict['dag']),
        mail_from="airflow@hdata.med.br",
    )

    # Send the email
    r = message.send(
        to='raphael.queiroz@eximio.med.br',
        smtp={
            "host": "email-smtp.us-east-2.amazonaws.com",
            "port": 587,
            "timeout": 5,
            "user": os.environ['AWS_SMTP_USERNAME'],
            "password": os.environ['AWS_SMTP_PASSWORD'],
            "tls": True,
        },
    )

    # Check if the email was properly sent
    assert r.status_code == 250
