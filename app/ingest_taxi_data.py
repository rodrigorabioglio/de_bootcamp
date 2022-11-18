#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import sqlalchemy
import os
import argparse

def main(args):

    filename = args.filename
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    dbname = args.dbname
    table_name = args.table_name

    batch_size = 10 ** 5
    i = 0

    df = pd.read_parquet(filename)
    chunks = df.shape[0] // 10 ** 5 + 1
    engine = sqlalchemy.create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    while i < chunks:
        to_db = df.iloc[i * batch_size:(i + 1) * batch_size, :]
        to_db.to_sql(table_name, con=engine, if_exists='append')
        print(f"sent chunk number {i+1}")
        i = i + 1

    print('finished')

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--filename", help="filename to be uploaded to db, must be in same directory")
    parser.add_argument("--user", help="PG user to connect")
    parser.add_argument("--password", help="PG password to connect")
    parser.add_argument("--host", help="PG host")
    parser.add_argument("--port", help="pg port")
    parser.add_argument("--dbname", help="db name")
    parser.add_argument("--table_name", help="table name")

    args = parser.parse_args()
    main(args)