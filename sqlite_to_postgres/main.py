import os
import csv
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from dataclasses import fields
from contextlib import closing
from dotenv import load_dotenv
from extract_to_csv import extract
from map_tables import map_tables
from pendulum import DateTime, parse
from utils import file_rename
from logger import logger

def upload(csv_file_name, map_columns, conn_params, schema, table_name):
    df = pd.read_csv(csv_file_name)
    df = df.rename(columns=map_columns)
    cols = ','.join(list(df.columns))
    insert_cr = f"INSERT INTO {table_name} ({cols}) VALUES " + "{cr_val};"
    url = URL.create(**conn_params)

    with closing(create_engine(url).connect()) as conn:
        conn.execute(f"truncate {schema + '.' + table_name} cascade;")
        count = df.shape[0]
        if count > 0:

            i = 0
            step = int(count / 100)
            if step == 0:
                step = count

            while i <= count:

                chunk = df.loc[i:i + step]
                num_rows = chunk.to_sql(con=conn, name=table_name, if_exists='append', index=False, method='multi', schema=schema)
                logging.info(f"Number of rows is added: {num_rows}")

                i += step + 1


load_dotenv()
db_sqlite = os.environ.get('SQLITE_DB_PATH', 'db.sqlite')
extract_filename = os.environ.get('EXTRACT_FILE_NAME', 'extract.csv')
upload_file_name = os.environ.get('UPLOAD_FILE_NAME', 'upload.csv')
conn_params = {
    'drivername': 'postgresql+psycopg2',
    'database': os.environ.get('PG_DB_NAME', 'postgres'),
    'username': os.environ.get('PG_USER', 'postgres'),
    'password': os.environ.get('PG_PASSWORD', 'postgres'),
    'host': os.environ.get('PG_HOST', 'localhost'),
    'port': os.environ.get('PG_PORT', 'postgres'),
}
schema = os.environ.get('PG_SCHEMA', 'public')
table_extracted_files = extract(db_sqlite, extract_filename, map_tables)
for table in map_tables:
    table_name = table[0]
    columns = table[2]
    Dataclass = table[3]
    map_columns = dict(zip(table[1], columns))
    field_types = {field.name: field.type for field in fields(Dataclass)}
    file_name = table_extracted_files[table_name]
    with closing(open(file_name, 'r', encoding="utf-8", newline='')) as extracted_file:
        csv_reader = csv.reader(extracted_file, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
        file_name = file_rename(upload_file_name, table_name)
        with closing(open(file_name, 'w', encoding="utf-8", newline='')) as upload_file:
            csv_writer = csv.writer(upload_file, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
            csv_writer.writerow(next(csv_reader, None))
            for row in csv_reader:
                kwargs = dict(zip(columns, row))
                dataclass = Dataclass(**kwargs)
                try:
                    for field, typo in field_types.items():
                        value = getattr(dataclass, field)
                        if bool(value):
                            if typo == DateTime:
                                parse(value)
                            else:
                                typo(value)
                except (ValueError, TypeError) as e:
                    logger.info(f"Value error of id({dataclass.id}) field({field}) by value ({value}) from file {str(extracted_file.name)} - {e}")
                    continue
                csv_writer.writerow(row)
    upload(upload_file.name, map_columns, conn_params, schema, table_name)