#!/usr/bin/env python
# coding: utf-8


# import urllib.request
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'

    try:
        # os.system(f'wget {url} -O {csv_name}')
        os.system(f'wget {url} -O - | gzip -d > {csv_name}')
        # urllib.request.urlretrieve(url, csv_name)

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

        df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

        df.to_sql(name=table_name, con=engine, if_exists='append')

        while True:
            try:
                t_start = time()

                df = next(df_iter)
                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
                # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

                df.to_sql(name=table_name, con=engine, if_exists='append')

                t_end = time()

                print('insert another chunk of data, took %.3f seconds' % (t_end - t_start))

            except StopIteration:
                # No hay más bloques de datos, salir del bucle
                print("Todos los datos han sido procesados.")
                break
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Siempre elimina el archivo descargado y descomprimido, incluso si hay una excepción
        if os.path.exists(csv_name):
            os.remove(csv_name)

        # Cierra la conexión a la base de datos
        engine.dispose()
        print("Recursos liberados.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest csv data to postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)

