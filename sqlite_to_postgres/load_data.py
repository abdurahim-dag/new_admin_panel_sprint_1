"""
Основной модуль выгрузки, трансформирования и загрузки данных
 из БД sqlite в БД Postgres.
"""
import os

from dotenv import load_dotenv

from extract import Extract
from load import Load
from map_tables import map_tables
from transform import Transform
from utils import file_rename

load_dotenv()
db_sqlite = os.environ.get('SQLITE_DB_PATH', 'db.sqlite')
extract_filename = os.environ.get('EXTRACT_FILE_NAME', 'extract.csv')
upload_filename = os.environ.get('UPLOAD_FILE_NAME', 'upload.csv')
conn_params = {
    'drivername': 'postgresql+psycopg2',
    'database': os.environ.get('PG_DB_NAME', 'postgres'),
    'username': os.environ.get('PG_USER', 'postgres'),
    'password': os.environ.get('PG_PASSWORD', 'postgres'),
    'host': os.environ.get('PG_HOST', 'localhost'),
    'port': os.environ.get('PG_PORT', 'postgres'),
}
schema = os.environ.get('PG_SCHEMA', 'public')

extract = Extract(db_path=db_sqlite)
transform = Transform()
load = Load(conn_params, schema)

for table in map_tables:
    table_name = table[0]
    dataclass_target = table[1]
    dataclass_source = table[2]

    extracted_filename = file_rename(extract_filename, table_name)
    extract.extract(table_name, dataclass_source, extracted_filename)

    file_name = file_rename(upload_filename, table_name)
    transform.transform(extracted_filename, file_name, dataclass_target)

    load.upload(file_name, table_name)
