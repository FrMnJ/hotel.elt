import pandas as pd
import os
from command import Command

class JsonLoadCommand(Command):
    def __init__(self, logger, file_path):
        super().__init__(logger)
        self.file_path = file_path

    def execute(self, data: pd.DataFrame):
        try:
            data.to_json(self.file_path, orient="records", indent=4)
            self.logger.write_line(f"Datos guardados en formato JSON en: {self.file_path}")
        except Exception as e:
            self.logger.write_line(f"Error al guardar en JSON: {e}")
            self.undo()

    def undo(self):
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
            self.logger.write_line(f"Archivo JSON eliminado: {self.file_path}")
