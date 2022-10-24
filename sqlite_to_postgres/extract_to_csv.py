import logging
import csv
import sqlite3
from contextlib import closing
from utils import file_rename

def extract(db_path: str, csv_filename: str, map_tables: tuple):
    sql = 'select {columns} from {table};'
    sql_count = 'select count(*) from {table};'
    result = {}

    with closing(sqlite3.connect(db_path)) as conn:
        with closing(conn.cursor()) as curs:
            for table in map_tables:
                table_name = table[0]
                columns = table[1]
                exec_sql = sql_count.format(table=table_name)
                curs.execute(exec_sql)
                count = int(curs.fetchone()[0])
                if count > 0:
                    filename = file_rename(csv_filename, table_name)
                    with closing(open(filename, 'w', encoding="utf-8", newline='')) as f:
                        writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
                        columns_str = ','.join(columns)
                        exec_sql = sql.format(columns=columns_str, table=table_name)
                        curs.execute(exec_sql)
                        i = 0
                        step = int(count / 100)
                        if step == 0:
                            i = count
                            step = count
                        writer.writerow(columns)
                        while i <= count:
                            rows = curs.fetchmany(step)
                            writer.writerows(rows)
                            i += step
                logging.info(f"Extracted {count} rows of table {table_name}")
                result[table_name] = filename

    return result
