import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from contextlib import closing

class Load:
    def __init__(self, conn_params, schema):
        self.url = URL.create(**conn_params)
        self.schema=schema


    def upload(self, csv_file_name: str, table_name: str):
        """

        """
        df = pd.read_csv(csv_file_name)
        with closing(create_engine(self.url).connect()) as conn:
            conn.execute(f"truncate {self.schema + '.' + table_name} cascade;")
            count = df.shape[0]
            if count > 0:
                i = 0
                step = int(count / 100)
                if step == 0:
                    step = count
                while i <= count:
                    chunk = df.loc[i:i + step]
                    num_rows = chunk.to_sql(
                        con=conn,
                        name=table_name,
                        if_exists='append',
                        index=False,
                        method='multi',
                        schema=self.schema
                    )
                    logging.info(f"Number of rows is added: {num_rows}")
                    i += step + 1
