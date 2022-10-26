import os

from utils import file_rename
from extract import Extract
from transform import Transform
from load import Load
from map_tables import map_tables
from dotenv import load_dotenv

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
    columns_source = table[1]
    columns_target = table[2]
    Dataclass = table[3]

    extracted_filename = file_rename(extract_filename, table_name)
    extract.extract(table_name, columns_source, extracted_filename)

    filename = file_rename(upload_filename, table_name)
    transform.transform(extracted_filename, filename, columns_target, Dataclass)

    load.upload(filename, table_name)