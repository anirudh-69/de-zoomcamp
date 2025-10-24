import argparse, os, sys
from time import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from pandas.io.sql import get_schema

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    tb = params.tb
    url = params.url

    file_name = url.rsplit('/',1)[-1].strip()
    print(f'Downloading {file_name} ...')
    os.system(f'curl {url.strip()} -o {file_name}')
    print('\n')

    engine = create_engine(f'clickhouse://{user}:{password}@{host}:{port}/{db}')

    # Use pandas to infer the schema and create a DDL statement
    # This is more robust than manually defining columns
    parquet_file = pq.ParquetFile(file_name)
    df_head = next(parquet_file.iter_batches(batch_size=10)).to_pandas().head(n=0)
    df_iter = pq.ParquetFile(file_name).iter_batches(batch_size=100000)
    
    # Generate the column definitions part of the CREATE TABLE statement
    # The `get_schema` function is a reliable way to do this
    # We call it without a connection to get a generic DDL statement
    generic_ddl = get_schema(df_head, name=tb)
    # We then replace the generic table creation with our ClickHouse-specific one
    create_table_ddl = generic_ddl.replace(f'CREATE TABLE "{tb}"', f'CREATE TABLE {db}.{tb}') \
                                .replace('"', '`') + " ENGINE = MergeTree() ORDER BY tuple();"
    
    with engine.connect() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {tb}"))
        connection.execute(text(create_table_ddl))

    t_start = time()
    count = 0
    for batch in df_iter:
        count += 1

        # This conversion is necessary because we're iterating over Arrow batches
        batch_df = batch.to_pandas()
        print(f'inserting batch {count} ...')

        b_start = time()
        batch_df.to_sql(name=tb, con=engine, if_exists='append', index=False)
        b_end= time()

        print(f'inserted! time taken {b_end - b_start:10.3f} seconds.\n')
    
    t_end = time()
    print(f'Completed! Total time taken {t_end - t_start:10.3f} seconds for {count} batches.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Loading data from .parquet file link to a Postgres database.')

    parser.add_argument('--user', help='Username for ClickHouse.')
    parser.add_argument('--password', help='Password to the username for ClickHouse.')
    parser.add_argument('--host', help='Hostname for ClickHouse.')
    parser.add_argument('--port', help='Port for ClickHouse connection.')
    parser.add_argument('--db', help='Database name for ClickHouse.')
    parser.add_argument('--tb', help='Destination table name for ClickHouse')
    parser.add_argument('--url', help='URL for .parquet file to download.')

    args = parser.parse_args()
    main(args)
