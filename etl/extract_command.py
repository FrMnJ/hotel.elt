import pandas as pd
from command import Command

class ExtractCommand(Command):
    def __init__(self, logger, dataset_path):
        super().__init__(logger)
        self.dataset_path = "./datasets/"+dataset_path
        self.data = None

    def execute(self):
        try:
            self.logger.write_line(f"Extrayendo datos desde: {self.dataset_path}")
            if self.dataset_path.endswith(".csv"):
                self.data = pd.read_csv(self.dataset_path, encoding="utf-8")
            elif self.dataset_path.endswith(".json"):
                self.data = pd.read_json(self.dataset_path, encoding="utf-8")
            elif self.dataset_path.endswith(".xlsx"):
                self.data = pd.read_excel(self.dataset_path)
                self.data = self.data.apply(lambda x: x.str.encode('utf-8').str.decode('utf-8') if x.dtype == 'object' else x)
            else:
                raise ValueError("Formato de archivo no soportado.")
            self.logger.write_line("Datos extraídos correctamente.")
            return self.data
        except Exception as e:
            self.logger.write_line(f"Error durante la extracción: {e}")
            raise

    def undo(self):
        self.logger.write_line("No se puede deshacer la extracción de datos.")