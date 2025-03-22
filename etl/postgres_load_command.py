import pandas as pd
import psycopg2 as pg

from etl.command import Command

class PostgresLoadCommand(Command):
    def __init__(self, conn, logger):
        super().__init__(logger)
        self.conn = conn

    def execute(self, data: pd.DataFrame):
        try:
            pd.to_sql(data, self.conn, if_exists='replace')
        except Exception as e:
            self.logger.write_line(e)
            self.undo()

    def undo(self):
        self.conn.rollback()
