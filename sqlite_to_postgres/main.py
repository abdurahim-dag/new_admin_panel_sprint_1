import logging
import os
import csv
from dataclasses import fields
from contextlib import closing
from dotenv import load_dotenv
from extract_to_csv import extract
from map_tables import map_tables

class DataClass:

    def __int__(self, Dataclass, columns, row):

# create logger
logger = logging.getLogger('simple_example')
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
# add ch to logger
logger.addHandler(ch)

load_dotenv()
db_sqlite = os.environ.get('SQLITE_DB_PATH', 'db.sqlite')
csv_filename = os.environ.get('FILE_CSV', 'extract.csv')
table_csvfile = extract(db_sqlite, csv_filename, map_tables)
for table in map_tables:
    table_name = table[0]
    columns = table[2]
    Dataclass = table[3]
    field_types = {field.name: field.type for field in fields(Dataclass)}
    csvfile = table_csvfile[table_name]
    with closing(open(csvfile, 'r', encoding="utf-8", newline='')) as f:
        reader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
        for row in reader:
            kwargs = dict(zip(columns, row))
            dataclass = Dataclass(**kwargs)
            try:
                for field, typo in field_types.items():
                    value = getattr(dataclass, field)
                    typo(value)
            except (ValueError, TypeError) as e:
                logger.info(f"Value error of id({dataclass.id}) field: {field} by value {value} from file {csvfile} - {e}")
                continue
            print(dataclass)