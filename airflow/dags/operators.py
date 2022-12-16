import pandas as pd

from pyarrow import concat_tables
import pyarrow.csv as pv
import pyarrow.parquet as pq

import psycopg2

from const import execution_dict

import sqlalchemy

import requests
import shutil

import os
import re


def download_files_and_unzip(path, filename='temp.zip', **kwargs):
    election_round_code = ['051020221321', '311020221535']
    state = execution_dict[kwargs['ds']]
    filename = f'{state}_{filename}'

    print(state)

    for i, c in enumerate(election_round_code, 1):

        # defining user-agent to bypass website bot gates
        headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36'}
        url = f'https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2022/buweb/bweb_{i}t_{state}_{c}.zip'
        print(url)
        r = requests.get(url=url,headers=headers)   

        if not path.endswith('/'):
            path = path + '/'

        with open(path + filename, 'wb') as f:
            f.write(r.content)

        shutil.unpack_archive(path + filename, path)

        os.system(f'rm {path}*.pdf {path}{filename}')


def csv_to_parquet(path, delimiter=';', encoding='Latin 1', **kwargs):
    parse_options = pv.ParseOptions(delimiter=delimiter)
    read_options = pv.ReadOptions(encoding=encoding)

    if not path.endswith('/'):
        path = path + '/'

    state = execution_dict[kwargs['ds']]
    files = [f for f in os.listdir(path) if re.search(f't_{state}_\d+.csv', f)]

    list_tables = []

    for f in files:
        tmp = pv.read_csv(
            path + f,
            read_options=read_options,
            parse_options=parse_options
        )

        list_tables.append(tmp)

        del tmp

    table = concat_tables(list_tables)

    pq.write_table(table, path + f'{state}_dois_turnos.parquet')


def prepare_data(path, **kwargs):
    cols = ['TS_GERACAO', 'NR_TURNO', 'SG_UF', 'CD_MUNICIPIO', 'NM_MUNICIPIO', 'NR_ZONA',
            'NR_SECAO', 'NR_LOCAL_VOTACAO', 'CD_CARGO_PERGUNTA',
            'DS_CARGO_PERGUNTA', 'NR_PARTIDO', 'SG_PARTIDO', 'NM_PARTIDO',
            'DT_BU_RECEBIDO', 'QT_APTOS', 'QT_COMPARECIMENTO', 'QT_ABSTENCOES',
            'CD_TIPO_URNA', 'DS_TIPO_URNA', 'CD_TIPO_VOTAVEL', 'DS_TIPO_VOTAVEL',
            'NR_VOTAVEL', 'NM_VOTAVEL', 'QT_VOTOS']

    if not path.endswith('/'):
        path = path + '/'

    state = execution_dict[kwargs['ds']]
    files = [f for f in os.listdir(path) if re.search(f't_{state}_\d+.csv', f)]

    list_df = []

    for f in files:
        tmp = pd.read_csv(
            path + f,
            sep=';',
            encoding='Latin 1'
        )

        list_df.append(tmp)

    df = pd.concat(list_df)

    prepared = df.assign(
        TS_GERACAO=lambda df: pd.to_datetime(df.DT_GERACAO + ' ' + df.HH_GERACAO.astype(str)),
        DT_BU_RECEBIDO=lambda df: pd.to_datetime(df.DT_BU_RECEBIDO)
    ) \
        .drop(columns=['DT_GERACAO', 'HH_GERACAO']) \
        [cols].rename(columns=lambda c: c.lower().strip())

    print(prepared.shape[0])
    filepath = path + state + '_dois_turnos'
    prepared.to_csv(filepath, sep=';', encoding='utf-8', index=False)
    del prepared
    print('prepared data was written')


def upload_parquet_to_postgres(
        connect_dict,
        path,
        table_name,
        if_exists='append',
        batch_size=10 ** 5,
        **kwargs):
    if not path.endswith('/'):
        path = path + '/'

    state = execution_dict[kwargs['ds']]
    filepath = path + state + '_dois_turnos'

    user = connect_dict['user']
    password = connect_dict['password']
    host = connect_dict['host']
    port = connect_dict['port']
    dbname = connect_dict['dbname']

    conn = psycopg2.connect(
        host=host,
        port=port,
        database=dbname,
        user=user,
        password=password)

    cur = conn.cursor()

    check_table_query = f"""
    SELECT EXISTS(SELECT 1 FROM information_schema.tables 
              WHERE table_catalog='{dbname}' AND 
                    table_schema='public' AND 
                    table_name='{table_name}')
    """
    cur.execute(check_table_query)
    table_exists = cur.fetchone()[0]

    if not table_exists:
        table_schema = pd.read_csv(filepath, sep=';', nrows=1).head(0)
        engine = sqlalchemy.create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
        table_schema.to_sql(table_name, con=engine, if_exists=if_exists, index=False)

    with open(filepath, 'r') as f:
        command = f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ';' ENCODING 'utf-8' NULL '' "
        cur.copy_expert(command, f)
        conn.commit()
        cur.close()

def delete_files(path, **kwargs):
    if not path.endswith('/'):
        path = path + '/'

    state = execution_dict[kwargs['ds']]
    files = [f for f in os.listdir(path) if re.search(f'{state}_', f)]

    for f in files:
        os.system(f'rm {path}{f}')
