import pandas as pd
from command import Command

class CleanDataCommand(Command):
    def __init__(self, logger):
        super().__init__(logger)

    def execute(self, data: pd.DataFrame):
        try:
            self.logger.write_line("Iniciando limpieza de datos...")

            # Normalizar los nombres de las columnas a minúsculas
            data.columns = data.columns.str.lower()
            self.logger.write_line("Nombres de columnas normalizados a minúsculas.")

            # Eliminar columnas innecesarias
            columns_to_drop = ['reservationstatus', 'arrivaldateyear']  # Ejemplo de columnas a eliminar
            for column in columns_to_drop:
                if column in data.columns:
                    data.drop(columns=[column], inplace=True)
                    self.logger.write_line(f"Columna '{column}' eliminada.")
                else:
                    self.logger.write_line(f"La columna '{column}' no está presente en el dataset.")

            # Convertir celdas vacías o espacios en blanco en NaN
            data.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
            self.logger.write_line("Celdas vacías convertidas a valores nulos (NaN).")

            # Reemplazar valores nulos según lo observado en los notebooks
            columns_to_fill = {
                'leadtime': 0,
                'meal': 'SC',
                'deposittype': 'No Deposit',
                'agent': 0,
                'company': 0,
                'children': 0,
                'country': 'Unknown',
                'adr': 'mean'
            }
            for column, value in columns_to_fill.items():
                if column in data.columns:
                    if value == 'mean' and column == 'adr':
                        mean_adr = data['adr'].mean()
                        data['adr'].fillna(mean_adr, inplace=True)
                        self.logger.write_line(f"Valores nulos en 'adr' reemplazados con el promedio: {mean_adr:.2f}.")
                    else:
                        data[column].fillna(value, inplace=True)
                        self.logger.write_line(f"Valores nulos reemplazados en '{column}' con '{value}'.")
                else:
                    self.logger.write_line(f"La columna '{column}' no está presente en el dataset.")

            # Eliminar filas duplicadas
            duplicates = data.duplicated().sum()
            if duplicates > 0:
                data.drop_duplicates(inplace=True)
                self.logger.write_line(f"Se eliminaron {duplicates} filas duplicadas.")
            else:
                self.logger.write_line("No se encontraron filas duplicadas.")

            # Renombrar columnas
            rename_map = {
                'adr': 'average_daily_rate',
                'leadtime': 'lead_time'
            }
            data.rename(columns=rename_map, inplace=True)
            self.logger.write_line(f"Columnas renombradas: {rename_map}")

            # Crear nuevas columnas basadas en otras columnas
            if 'staysinweekendnights' in data.columns and 'staysinweeknights' in data.columns:
                data['totalnights'] = data['staysinweekendnights'] + data['staysinweeknights']
                self.logger.write_line("Columna 'totalnights' creada sumando 'staysinweekendnights' y 'staysinweeknights'.")
            else:
                self.logger.write_line("No se pudo crear la columna 'totalnights' porque faltan columnas necesarias.")

            # Ordenar los datos por la columna 'totalnights'
            if 'totalnights' in data.columns:
                data.sort_values(by='totalnights', ascending=False, inplace=True)
                self.logger.write_line("Datos ordenados por la columna 'totalnights'.")

            self.logger.write_line("Limpieza de datos completada.")
            return data
        except Exception as e:
            self.logger.write_line(f"Error durante la limpieza: {e}")
            return data  # Devolver el DataFrame original en caso de error

    def undo(self):
        self.logger.write_line("No se puede deshacer la limpieza de datos.")