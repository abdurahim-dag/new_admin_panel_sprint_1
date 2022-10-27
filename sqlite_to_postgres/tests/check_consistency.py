import psycopg2
import sqlite3
import dataclasses
import os
from dotenv import load_dotenv
from contextlib import closing
from sqlite_to_postgres.map_tables import map_tables


class TestSolution:
    """

    """
    def __init__(self):
        load_dotenv()
        self.conn_params = {
            'database': os.environ.get('PG_DB_NAME', 'postgres'),
            'user': os.environ.get('PG_USER', 'postgres'),
            'password': os.environ.get('PG_PASSWORD', 'postgres'),
            'host': os.environ.get('PG_HOST', 'localhost'),
            'port': os.environ.get('PG_PORT', 'postgres'),
        }
        self.db_path = '../' + os.environ.get('SQLITE_DB_PATH', 'db.sqlite')
        self.map_tables = map_tables
        self.sql_count = 'select count(*) from {table};'
        self.sql_all = 'select {columns} from {table};'
        self.sql_byid = 'select {columns} from {table} where id={id};'
        self.schema = os.environ.get('PG_SCHEMA', 'public')

    def pg_connect(self):
        """

        Returns:

        """
        with closing(psycopg2.connect(**self.conn_params)) as conn:
            with closing(conn.cursor()) as curs:
                while True:
                    exec_sql = yield
                    curs.execute(exec_sql)
                    yield curs.fetchone()


    def sqlite_connect(self):
        """

        Returns:

        """
        with closing(sqlite3.connect(self.db_path)) as conn:
            with closing(conn.cursor()) as curs:
                while True:
                    exec_sql = yield
                    curs.execute(exec_sql)
                    yield curs.fetchone()


    def load_dataclass(self, dataclass, row):
        columns = [field.name for field in dataclasses.fields(dataclass)]
        kwargs = dict(zip(columns, row))
        dc = dataclass(**kwargs)
        return dc



    def test_tables_counts(self):
        pg = self.pg_connect()
        sqlite = self.sqlite_connect()
        for table in self.map_tables:
            next(pg)
            sql = self.sq.format(table=table[0])
            count_pg = int(pg.send(sql)[0])
            next(sqlite)
            count_sqlite = int(sqlite.send(sql)[0])
            assert count_sqlite == count_pg
        pg.close()
        sqlite.close()

    def test_tables_row(self):
        pg = self.pg_connect()
        sqlite = self.sqlite_connect()
        for table in self.map_tables:
            dc_pg = table[1]
            dc_sqlite = table[2]

            next(sqlite)

            fileds_source = [field.name for field in dataclasses.fields(dc_sqlite)]
            columns_source_str = ','.join(fileds_source)

            sql_all = self.sql_all.format(columns=columns_source_str, table=table[0])
            rows_source = sqlite.send(sql_all)
            for row_source in rows_source:

                kwargs = dict(zip(fileds_source, row_source))
                dc_source = dc_sqlite(**kwargs)

                next(pg)
                fileds_target = [field.name for field in dataclasses.fields(dc_pg)]
                columns_target_str = ','.join(fileds_target)

                sql = self.sql_byid.format(columns=columns_target_str, table=table[0], id=row_source[0])
                check_row = pg.send(sql)

                kwargs = dict(zip(fileds_target, check_row))
                dc_target = dc_pg(**kwargs)

                for filed_source in fileds_source:
                    for filed_target in fileds_target:
                        value_source = getattr(dc_source, filed_source)
                        value_target = getattr(dc_target, filed_target)
                        assert value_source == value_target
        pg.close()
        sqlite.close()


test = TestSolution()
test.test_tables_row()