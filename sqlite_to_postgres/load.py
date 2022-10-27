"""Модуль загрузки csv файла в таблицу."""

import logging
from contextlib import closing

import pandas as pd
from logger import logger
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

class Load:
    """
    Класс отвечающий за загрузку.
    """

    def __init__(self, conn_params: dict, schema: str) -> None:
        """Устанавливаем целевые параметры соединения и схему БД.

        Args:
            conn_params (dict): Параметры соединения с БД.
            schema (str): Схема БД.
        """
        self.url = URL.create(**conn_params)
        self.schema = schema

    def upload(self, csv_file_name: str, table_name: str) -> None:
        """Загружаем пачками строки из файла в таблицу.

        Args:
            csv_file_name (str): Загружаемый файл csv.
            table_name (str): Целевая таблица, для загрузки.
        """
        df = pd.read_csv(csv_file_name)
        with closing(create_engine(self.url).connect()) as conn:
            # Очищаем таблицу.
            conn.execute(f"truncate {self.schema + '.' + table_name} cascade;")
            # Если строк в файле 0, то идём дальше.
            count = df.shape[0]
            if count > 0:
                i = 0
                step = int(count / 100)
                if step == 0:
                    step = count
                # До опустошения загружаем стоки из файла в таблицу.
                while i <= count:
                    chunk = df.loc[i:i + step]
                    num_rows = chunk.to_sql(
                        con=conn,
                        name=table_name,
                        if_exists='append',
                        index=False,
                        method='multi',
                        schema=self.schema,
                    )
                    logging.info(f"Number of rows is added: {num_rows}")
                    i += step + 1
        logger.info(f"File {csv_file_name} completed load to table {table_name}")
