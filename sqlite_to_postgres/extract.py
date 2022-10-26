import csv
import sqlite3

from contextlib import closing
from logger import logger


class Extract:
    """

    """
    def __init__(self, db_path: str):
        """

        """
        self.db_path = db_path
        self.sql = 'select {columns} from {table};'
        self.sql_count = 'select count(*) from {table};'

    def extract(self, table_name: str, columns: tuple,  csv_filename: str):
        """

        """
        columns_str = ','.join(columns)
        with closing(sqlite3.connect(self.db_path)) as conn:
            with closing(conn.cursor()) as curs:
                exec_sql = self.sql_count.format(table=table_name)
                curs.execute(exec_sql)
                count = int(curs.fetchone()[0])
                if count > 0:
                    with closing(open(csv_filename, 'w', encoding="utf-8", newline='')) as f:
                        writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
                        exec_sql = self.sql.format(columns=columns_str, table=table_name)
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
                        logger.info(f"Extracted {count} rows of table {table_name}")
