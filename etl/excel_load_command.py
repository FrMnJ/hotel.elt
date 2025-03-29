import pandas as pd
import os
from command import Command

class ExcelLoadCommand(Command):
    def __init__(self, logger, file_path):
        super().__init__(logger)
        self.file_path = file_path

    def execute(self, data: pd.DataFrame):
        try:
            data.to_excel(self.file_path, index=False)
            self.logger.write_line(f"Datos guardados en formato Excel en: {self.file_path}")
        except Exception as e:
            self.logger.write_line(f"Error al guardar en Excel: {e}")
            self.undo()
    
    def undo(self):
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
            self.logger.write_line(f"Archivo Excel eliminado: {self.file_path}")
