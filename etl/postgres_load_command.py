import pandas as pd
from sqlalchemy import create_engine
from command import Command

class PostgresLoadCommand(Command):
    def __init__(self, conn_string, table_name, logger):
        super().__init__(logger)
        self.conn_string = conn_string
        self.table_name = table_name

    def execute(self, data: pd.DataFrame):
        try:
            engine = create_engine(self.conn_string)
            data.to_sql(self.table_name, engine, if_exists="replace", index=False)
            self.logger.write_line(f"Datos guardados en la tabla '{self.table_name}' de PostgreSQL.")
        except Exception as e:
            self.logger.write_line(f"Error al guardar en PostgreSQL: {e}")
            self.undo()

    def undo(self):
        self.logger.write_line("No se puede deshacer la operación en PostgreSQL.")
