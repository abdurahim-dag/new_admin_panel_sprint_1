"""
Модуль выгрузки даных из БД sqlite и заданной таблицы.
"""
import csv
import dataclasses
import sqlite3
from contextlib import closing

from logger import logger


class Extract:
    """
    Класс выгружает данные из таблицы, как есть.
    """
    def __init__(self, db_path: str):
        """
        Конструктор класса.

        Args:
            db_path (str): Путь до БД sqlite.
        """
        self.db_path = db_path
        self.sql = 'select {columns} from {table};'
        self.sql_count = 'select count(*) from {table};'

    def extract(self, table_name: str, dataclass: dataclasses.dataclass, csv_filename: str):
        """
        Функция выгрузки таблицы из БД в файл csv.

        Args:
            table_name (str): название таблицы.
            dataclass (dataclass): Описание структуры целевой таблицы.
            csv_filename (str): название файла csv, куда будем выгружать.
        """
        columns = [field.name for field in dataclasses.fields(dataclass)]
        columns_str = ','.join(columns)
        with closing(sqlite3.connect(self.db_path)) as conn:
            with closing(conn.cursor()) as curs:
                # Если строк в таблице 0, то идём дальше.
                exec_sql = self.sql_count.format(table=table_name)
                curs.execute(exec_sql)
                count = int(curs.fetchone()[0])
                if count > 0:
                    # Записываем как есть данные в файл csv.
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
