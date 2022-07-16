import pandas as pd
import sqlite3
import numpy as np
import math
import boto3
from boto3.s3.transfer import TransferConfig
from io import BytesIO
import psycopg2 as ps


def start(file, source: str):
    base_df = pd.read_csv(file, sep=',', header=0)

    split_columns_df = transform_df(base_df)
    if source == 'lite':
        resp = db_commit(split_columns_df)
    else:
        csv_buffer = BytesIO()
        split_columns_df.to_csv(csv_buffer)
        resp = aws_commit(csv_buffer)
    return resp


def transform_df(df):
    # splitting origin and destination coordinates into separate columns
    # by first removing the POINTS( at the beginning and ) at the end and then
    # splitting based on the space between the coordinates and finally
    # removing the original 2 columns
    df['origin_coord'] = df['origin_coord'].str[7:-1]
    df[['origin_long', 'origin_lat']] = df['origin_coord'].str.split(' ', 1, expand=True)

    df['destination_coord'] = df['destination_coord'].str[7:-1]
    df[['destination_long', 'destination_lat']] = df['destination_coord'].str.split(' ', 1, expand=True)

    df.drop(['origin_coord', 'destination_coord'], axis=1, inplace=True)
    df = df[['region', 'origin_lat', 'origin_long', 'destination_lat', 'destination_long', 'datetime', 'datasource']]

    df['datetime'] = pd.to_datetime(df['datetime'])
    return df


def db_commit(df):
    # connecting to sqlite database, setting optimization parameters
    con = sqlite3.connect('jobsity.db', isolation_level=None)
    con.execute('PRAGMA journal_mode = OFF;')
    con.execute('PRAGMA synchronous = 0;')
    con.execute('PRAGMA cache_size = 1000000;')
    con.execute('PRAGMA locking_mode = EXCLUSIVE;')
    con.execute('PRAGMA temp_store = MEMORY;')

    df_length = len(df)

    # no need to chunk anything below 10k rows
    if df_length < 10000:
        df_chunkcount = 1
    # add a chunk for every order of magnitude of 10
    elif df_length < 1000000:
        df_chunkcount = math.ceil(math.log10(df_length))
    # incrementally increase chunk count based on natural logarithm
    else:
        # natural logarithm of dataframe length
        length_ln = math.log(df_length)
        # order of magnitute of 10 beyond 1,000,000
        length_oom = max(math.log10(df_length) - 6, 1)
        # chunk count rises exponentially, works until approximately 256 million where the chunk size starts falling below 200k records per operation
        df_chunkcount = length_ln ** length_oom

    # creating a list of dataframe chunks
    df_array = np.array_split(df, df_chunkcount)

    # processing individual chunks
    chunk = 0
    for frame in df_array:
        chunk += 1
        frame.to_sql('trips', con, if_exists='append', index=False)
        # providing status updates per chunk out of total
        yield 'Chunk ' + str(chunk) + ' uploaded, out of total ' + str(df_chunkcount)
    con.commit()
    con.close()
    yield 'All chunks processed'


def aws_commit(buffer):
    # connecting to Redshift
    ps_con = ps.connect(dbname='trips',
                        host='prod.triptripping.us-east-x.redshift.amazonaws.com',
                        port='9000',
                        user='usr',
                        password='superSecurePassword')

    session = boto3.Session(
        aws_access_key_id='xxx',
        aws_secret_access_key='xxx')

    s3 = session.client('s3')
    # setting file size, in this case 1GB before it is split to another file
    GB = 1024 ** 3
    config = TransferConfig(multipart_threshold=1 * GB)
    response = s3.upload_fileobj(buffer, 'trips-import', 'trips.csv', Config=config)

    # fastest way I've found to get data from S3 into Redshift
    sql = """COPY trips_import FROM 's3://trips-import/' credentials 
		  'aws_access_key_id=xxx;aws_secret_access_key=xxx'
		   csv delimiter ','; commit;"""

    cur = ps_con.cursor()
    cur.execute(sql)
    ps_con.commit()
    ps_con.close()
    return 1
