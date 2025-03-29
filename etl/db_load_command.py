import pandas as pd
import psycopg2
from urllib.parse import urlparse
from command import Command


class DBLoadCommand(Command):
    def __init__(self, logger, conn_string, table_name):
        super().__init__(logger)
        self.conn_string = conn_string
        self.table_name = table_name

    def execute(self, data: pd.DataFrame):
        try:
            # Limpiar datos para evitar problemas de codificación y reemplazar valores NaN
            data = data.fillna("NULL")
            data = data.applymap(lambda x: str(x).encode('utf-8', 'ignore').decode('utf-8') if isinstance(x, str) else x)
            
            # Analizar la cadena de conexión
            parsed = urlparse(self.conn_string)
            username = parsed.username
            password = parsed.password
            database = parsed.path[1:]  # eliminar el '/' inicial
            hostname = parsed.hostname
            port = parsed.port or 5432
            
            # Conectar con psycopg2 directamente
            conn = psycopg2.connect(
                host=hostname,
                database=database,
                user=username,
                password=password,
                port=port
            )
            
            # Crear un cursor
            cursor = conn.cursor()
            
            # Crear tabla si no existe
            columns = []
            for col_name, dtype in data.dtypes.items():
                # Convertir tipo de pandas a tipo PostgreSQL
                if pd.api.types.is_integer_dtype(dtype):
                    pg_type = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    pg_type = "FLOAT"
                elif pd.api.types.is_datetime64_dtype(dtype):
                    pg_type = "TIMESTAMP"
                else:
                    pg_type = "TEXT"
                # Asegurar que los nombres de columnas estén correctamente entrecomillados
                columns.append(f"\"{col_name}\" {pg_type}")
            
            # Depuración: Registrar las columnas que se están creando
            self.logger.write_line(f"Creating table with columns: {columns}")
            
            create_table_query = f"CREATE TABLE IF NOT EXISTS \"{self.table_name}\" ({', '.join(columns)})"
            self.logger.write_line(f"Creating table with query: {create_table_query}")
            cursor.execute(create_table_query)
            
            # Eliminar y recrear la tabla para asegurar un esquema limpio
            cursor.execute(f"DROP TABLE IF EXISTS \"{self.table_name}\"")
            cursor.execute(create_table_query)
            
            # Usar declaraciones SQL INSERT para mayor fiabilidad
            self.logger.write_line(f"Inserting {len(data)} rows into table {self.table_name}")
            
            # Preparar nombres de columnas para la inserción
            col_names_str = ', '.join([f"\"{col}\"" for col in data.columns])
            
            # Insertar datos en bloques para mejorar el rendimiento
            chunk_size = 500
            for i in range(0, len(data), chunk_size):
                chunk = data.iloc[i:i+chunk_size]
                values = []
                placeholders = []
                
                for _, row in chunk.iterrows():
                    row_values = []
                    row_placeholders = []
                    for val in row:
                        if val == "NULL":
                            row_placeholders.append("NULL")
                        else:
                            row_placeholders.append("%s")
                            row_values.append(val)
                    
                    placeholders.append(f"({', '.join(row_placeholders)})")
                    values.extend(row_values)
                
                insert_query = f"INSERT INTO \"{self.table_name}\" ({col_names_str}) VALUES {', '.join(placeholders)}"
                cursor.execute(insert_query, values)
            
            # Confirmar y cerrar
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.write_line(f"Datos guardados en la tabla '{self.table_name}' de base de datos.")
        except Exception as e:
            self.logger.write_line(f"Error al guardar en base de datos: {e}")
            # Para depuración: imprimir información de error más detallada
            import traceback
            self.logger.write_line(traceback.format_exc())
            self.undo()

    def undo(self):
        self.logger.write_line("No se puede deshacer la operación en bases de datos.")
