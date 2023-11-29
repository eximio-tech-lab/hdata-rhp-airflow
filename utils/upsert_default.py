import pandas as pd
import numpy as np

from sqlalchemy import DateTime, String
from dateutil import rrule
from connections.oracle.connections import connect_rhp as source_engine, connect_rhp_hdata as connect_hdata, connect_string

HOSPITAL = "RHP"

def update_cells(df_eq, table_name, CD):
    cols = df_eq.dtypes[df_eq.dtypes=='datetime64[ns]'].index
    d = df_eq.to_dict(orient='split')
    #print(d)
    for dado in d['data']:
        for i in range(len(dado) - 1):
            conn = connect_hdata()
            cursor = conn.cursor()

            query = ''
            query = 'UPDATE {nome_tabela} '.format(nome_tabela=table_name)
            if pd.isna(dado[i + 1]):
                query += 'SET {nome_coluna} = null '.format(nome_coluna=d['columns'][i + 1])
            else:
                print(type(dado[i + 1]))
                if type(dado[i + 1]) == np.int64 or type(dado[i + 1]) == np.float64 or type(dado[i + 1]) == int:
                    query += 'SET {nome_coluna} = {novo_valor} '.format(nome_coluna=d['columns'][i + 1],
                                                            novo_valor=dado[i + 1])
                elif d['columns'][i + 1] in cols:
                    query += 'SET {nome_coluna} = TIMESTAMP \'{novo_valor}\' '.format(nome_coluna=d['columns'][i + 1],
                                                            novo_valor=dado[i + 1])
                else:
                    query += 'SET {nome_coluna} = \'{novo_valor}\' '.format(nome_coluna=d['columns'][i + 1],
                                                            novo_valor=dado[i + 1])
            query += 'WHERE {cd} IN({todos_cds})'.format(cd=CD, todos_cds=dado[0])

            #print(query)
            cursor.execute(query)
            conn.commit()
            conn.close()

def check_table(tabela):
    conn = connect_hdata()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM all_tables
        WHERE owner = '{0}'
        AND TABLE_NAME = '{1}'
        """.format(HOSPITAL,tabela.replace('\'', '\'\'')))
    if cursor.fetchone()[0] == 1:
        cursor.close()
        return True
    else:
        cursor.close()
        return False

def simple_upsert(query_origem, tabela_destino, pk, mending_callback = '0'):
    df = pd.read_sql(query_origem, source_engine())
    df.columns = [x.upper() for x in df.columns]
    cols = df.dtypes[df.dtypes=='object'].index
    type_mapping = {col : DateTime if ('DT' in col or 'DAT' in col) else String(255) for col in cols }
    if not isinstance(mending_callback, str):
        df = mending_callback(df)
    if check_table(tabela_destino):
        df_stage = pd.read_sql("SELECT {pk} from {namespace}.{tabela}".format(namespace=HOSPITAL ,pk=pk, tabela=tabela_destino), connect_hdata())
        df_stage.columns = [x.upper() for x in df_stage.columns]
        print(df)
        print(df_stage)
        df_diff = df.merge(df_stage[pk],indicator = True, how='left').loc[lambda x : x['_merge'] !='both']
        df_diff = df_diff.drop(columns=['_merge'])
        df_diff = df_diff.reset_index(drop=True)
        df_diff.to_sql(tabela_destino, connect_string(), schema=HOSPITAL, if_exists='append', index=False, dtype=type_mapping)
    else:
        df.to_sql(tabela_destino, connect_string(), schema=HOSPITAL, if_exists='replace', index=False, dtype=type_mapping)

def by_date_upsert(inicio, fim, query_origem, tabela_destino, pk, mending_callback = '0'):
    for dt in rrule.rrule(rrule.DAILY, dtstart=inicio, until=fim):
        print(str(dt))
        # with pd.read_sql(query_origem.format(dt=dt.strftime('%d/%m/%Y')), source_engine(),  chunksize=10000, iterator) as charger:
        for df in pd.read_sql(query_origem.format(dt=dt.strftime('%d/%m/%Y')), source_engine(),  chunksize=10000):
            df.columns = [x.upper() for x in df.columns]
            if not df.empty:
                cols = df.dtypes[df.dtypes=='object'].index
                if not isinstance(mending_callback, str):
                    df = mending_callback(df)
                print(df.info())
                type_mapping = {col : DateTime if ('DT' in col or 'DAT' in col) else String(255) for col in cols }
                if check_table(tabela_destino):
                    con = connect_hdata()
                    cursor = con.cursor()
                    if int(len(df)) > 1:
                        pks = list(df[pk])
                        range_pk = int(len(pks) / 999) + 1
                        list_pks = [pks[i::range_pk] for i in range(range_pk)]
                        for seqs in list_pks:
                            cursor.execute('DELETE FROM {table} where {pk} in {seqs}'.format(table=tabela_destino,
                                                                                            pk=pk,
                                                                                            seqs=tuple(seqs)))
                            con.commit()
                    else:
                        cursor.execute('DELETE FROM {table} where {pk} = {seq}'.format(table=tabela_destino,
                                                                                            pk=pk,
                                                                                            seq=df[pk][0]))
                        con.commit()
                    con.close()
                    try:
                        df.to_sql(tabela_destino, connect_string(), schema=HOSPITAL, if_exists='append', index=False, dtype=type_mapping)
                    except Exception as e:
                        print(e)
                        print('Dataframe do erro: ')
                        print(df.to_string())
                        raise ValueError('Inserção interrompida')
                else:
                    df.to_sql(tabela_destino, connect_string(), schema=HOSPITAL, if_exists='replace', index=False, dtype=type_mapping)

def by_date_upsert_two_pk(inicio, fim, query_origem, tabela_destino, pk, pk2):
    for dt in rrule.rrule(rrule.DAILY, dtstart=inicio, until=fim):
        print(str(dt))
        df = pd.read_sql(query_origem.format(dt=dt.strftime('%d/%m/%Y')), source_engine())
        df.columns = [x.upper() for x in df.columns]
        print(df.info())
        if not df.empty:
            cols = df.dtypes[df.dtypes=='object'].index
            type_mapping = {col : DateTime if ('DT' in col or 'DAT' in col) else String(255) for col in cols }
            if check_table(tabela_destino):
                con = connect_hdata()
                cursor = con.cursor()
                if int(len(df)) > 1:
                    pks = list(df[[pk,pk2]].apply(tuple,axis=1))
                    range_pk = int(len(pks) / 999) + 1
                    list_pks = [pks[i::range_pk] for i in range(range_pk)]
                    for seqs in list_pks:
                        cursor.execute('DELETE FROM {table} where ({pk},{pk2}) in {seqs}'.format(table=tabela_destino,
                                                                                        pk=pk,
                                                                                        pk2=pk2,
                                                                                        seqs=tuple(seqs)))
                        con.commit()
                else:
                    cursor.execute('DELETE FROM {table} where {pk} = {seq} and {pk2} = {seq2}'.format(table=tabela_destino,
                                                                                        pk=pk,
                                                                                        pk2=pk2,
                                                                                        seq=df[pk][0],
                                                                                        seq2=df[pk2][0]))
                    con.commit()
                con.close()
                df.to_sql(tabela_destino, connect_string(), schema=HOSPITAL, if_exists='append', index=False, dtype=type_mapping)
            else:
                df.to_sql(tabela_destino, connect_string(), schema=HOSPITAL, if_exists='replace', index=False, dtype=type_mapping)

def by_pk_upsert(query_search_pk, query_origem, tabela_destino, pk, search_engine, mending_callback='0'):
    df_pks = pd.read_sql(query_search_pk, search_engine)
    if not isinstance(mending_callback, str):
        df_pks = mending_callback(df_pks)
    if not df_pks.empty:
        if check_table(tabela_destino):
            conn = connect_hdata()
            cursor = conn.cursor()
            pks = list(df_pks[pk])
            range_pk = int(len(pks) / 999) + 1
            list_pks = [pks[i::range_pk] for i in range(range_pk)]
            for seqs in list_pks:
                cursor.execute('DELETE FROM {schema}.{table} where {pk} in {seqs}'.format(schema=HOSPITAL,
                                                                                table=tabela_destino,
                                                                                pk=pk,
                                                                                seqs=str(tuple(seqs)).replace(',)',')')))
                conn.commit()
                df = pd.read_sql(query_origem.format(seqs=str(tuple(seqs)).replace(',)',')')),source_engine())
                if not df.empty:
                    df.columns = [x.lower() for x in df.columns]
                    cols = df.dtypes[df.dtypes=='object'].index
                    type_mapping = {col : DateTime if ('DT' in col or 'DAT' in col) else String(255) for col in cols }
                    df.to_sql(tabela_destino, connect_string(), schema=HOSPITAL, if_exists='append', index=False, dtype=type_mapping)
            conn.close()
        else:
            raise('TABELA NÃO EXISTE')