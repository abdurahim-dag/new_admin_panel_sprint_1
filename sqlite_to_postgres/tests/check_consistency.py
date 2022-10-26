import psycopg2
import sqlite3
import pytest
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
        self.schema = os.environ.get('PG_SCHEMA', 'public')

    def pg_connect(self):
        """

        Returns:

        """
        with closing(psycopg2.connect(**self.conn_params)) as conn:
            with closing(conn.cursor()) as curs:
                while True:
                    table_name = yield
                    table_name = self.schema + '.' + table_name
                    exec_sql = self.sql_count.format(table=table_name)
                    curs.execute(exec_sql)
                    yield curs.fetchone()[0]


    def sqlite_connect(self):
        """

        Returns:

        """
        with closing(sqlite3.connect(self.db_path)) as conn:
            with closing(conn.cursor()) as curs:
                while True:
                    table_name = yield
                    exec_sql = self.sql_count.format(table=table_name)
                    curs.execute(exec_sql)
                    yield curs.fetchone()[0]


    def test_tables_counts(self):
        pg = self.pg_connect()
        sqlite = self.sqlite_connect()
        for table in self.map_tables:
            next(pg)
            count_pg = int(pg.send(table[0]))
            next(sqlite)
            count_sqlite = int(sqlite.send(table[0]))
            assert count_sqlite == count_pg
        pg.close()
        sqlite.close()



test = TestSolution()
test.test_tables_counts()