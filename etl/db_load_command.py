import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from command import Command

# {db_connection}://{db_username}:{db_password}@{db_host}/{db_name}
# Ejemplo: postgresql://root:root@localhost

class DBLoadCommand(Command):
    def __init__(self,logger , conn_string, table_name):
        super().__init__(logger)
        self.conn_string = conn_string
        self.table_name = table_name

    def execute(self, data: pd.DataFrame):
        try:
            data = data.applymap(lambda x: str(x).encode('utf-8', 'ignore').decode('utf-8') if isinstance(x, str) else x)
            
            engine = create_engine(self.conn_string, connect_args={"options": "-c client_encoding=utf8"})
            data.to_sql(self.table_name, engine, if_exists="replace", index=False)
            self.logger.write_line(f"Datos guardados en la tabla '{self.table_name}' de base de datos.")
        except Exception as e:
            self.logger.write_line(f"Error al guardar en base de datos: {e}")
            self.undo()

    def undo(self):
        self.logger.write_line("No se puede deshacer la operación en bases de datos.")
