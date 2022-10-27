import dataclasses
import os
from sqlite3 import connect, Connection
from contextlib import closing
from pathlib import Path
from pathlib import PurePath
from typing import  Callable, Generator
import psycopg2
import pytest
from dotenv import load_dotenv
from pendulum import DateTime
from pendulum import parse

from sqlite_to_postgres.map_tables import map_tables


class TestDB:
    """
    Pytest класс проверки на равенства значений и количества строк в таблицах.
    """
    @pytest.fixture
    def init(self) -> None:
        """
        Фикстура для инициализации начальных значений.
        """
        load_dotenv()
        conn_params = {
            'database': os.environ.get('PG_DB_NAME', 'postgres'),
            'user': os.environ.get('PG_USER', 'postgres'),
            'password': os.environ.get('PG_PASSWORD', 'postgres'),
            'host': os.environ.get('PG_HOST', 'localhost'),
            'port': os.environ.get('PG_PORT', 'postgres'),
        }
        # Проверка наличия файла БД sqlite выше в иерархии.
        schema = os.environ.get('PG_SCHEMA', 'public')
        p = Path(PurePath(Path('.'),
                          os.environ.get('SQLITE_DB_PATH', 'db.sqlite')))
        if p.exists:
            db_path = p.resolve()
        else:
            raise FileExistsError

        self.conn_params = conn_params
        self.db_path = db_path
        self.map_tables = map_tables
        self.sql_count = r'select count(*) from {table};'
        self.sql_all = r'select {columns} from {table};'
        self.sql_byid = r"select {columns} from {table} where id='{id}';"
        self.schema = schema

    @pytest.fixture
    def db_connect(self, init: Callable) -> Generator[int, float, str]:
        """
        Фиксутра, для нормального открытия и закрытия соединений в БД.

        Args:
            init(Callable): Выше стоящая фикстура инициализации.
        """
        with closing(psycopg2.connect(**self.conn_params)) as pg_conn:
            with closing(pg_conn.cursor()) as self.pg_curs:
                with closing(sqlite3.connect(self.db_path)) as sqlite_conn:
                    with closing(sqlite_conn.cursor()) as self.sqlite_curs:
                        yield


    def load_dataclass(self, dataclass, row):
        columns = [field.name for field in dataclasses.fields(dataclass)]
        kwargs = dict(zip(columns, row))
        dc = dataclass(**kwargs)
        return dc


    def get_dc(self, row, fields, dc):
        fields_list = [field.name for field in fields]
        kwargs = dict(zip(fields_list, row))
        return dc(**kwargs)


    def test_tables_counts(self, db_connect):
        check = {}
        for table in self.map_tables:
            sql = self.sql_count.format(table=table[0])
            self.pg_curs.execute(sql)
            row = self.pg_curs.fetchone()
            count_pg = int(row[0])

            self.sqlite_curs.execute(sql)
            row = self.sqlite_curs.fetchone()
            count_sqlite = int(row[0])
            check[table[0]] = (count_sqlite == count_pg), count_sqlite, count_pg

        message=[]
        asserted = True
        for k, v in check.items():
            if not v[0]:
                message.append(f"{k}: sqlite({v[1]}) pg({v[2]})")
            asserted = asserted & v[0]
        assert asserted, '|'.join(message)





    def get_typed_value(self, field, dc):
        value = getattr(dc, field.name)
        if bool(value):
            value = str(value)
            if field.type is DateTime:
                value = parse(value)
            else:
                value = field.type(value)
        return value

    def test_tables_row(self, db_connect):
        checks = []
        for table in self.map_tables:
            table_name = table[0]
            dc_pg = table[1]
            dc_sqlite = table[2]
            fields_dc_pg = [field for field in dataclasses.fields(dc_pg)]
            fields_dc_sqlite = [field for field in dataclasses.fields(dc_sqlite)]
            columns_pg = ','.join([field.name for field in fields_dc_pg])
            columns_sqlite = ','.join([field.name for field in fields_dc_sqlite])

            sql = self.sql_all.format(columns=columns_sqlite, table=table_name)
            self.sqlite_curs.execute(sql)

            for row_sqlite in self.sqlite_curs:
                dc1 = self.get_dc(row_sqlite, fields_dc_pg, dc_pg)

                sql = self.sql_byid.format(columns=columns_pg, table=table_name, id=row_sqlite[0])
                self.pg_curs.execute(sql)
                row_pg = self.pg_curs.fetchone()

                dc2 = self.get_dc(row_pg, fields_dc_pg, dc_pg)

                for field in fields_dc_pg:
                    value_1 = self.get_typed_value(field, dc1)
                    value_2 = self.get_typed_value(field, dc2)
                    if value_1 != value_2:
                        checks.append(f"{table_name}({field.name}): pg({value_1}), sqlit({value_2})")

        assert len(checks) == 0, " | ".join(checks)



# @pytest.mark.parametrize(('table_name','value1','value2'), db.tables_counts())
# def test_tables_counts(table_name, value1, value2):
#     assert value1 == value2

# @pytest.mark.parametrize(('table_name','value1','value2'), db.tables_row())
# def test_tables_counts(table_name, value1, value2):
#     assert value1 == value2
