from command import Command
from etl_factory import ETLFactory
class ETL(Command):
    def __init__(self, logger, etl_factory: ETLFactory):
        super().__init__(logger)
        self.extract = etl_factory.get_extract()
        self.transform = etl_factory.get_transform()
        self.load = etl_factory.get_load()
        self.history = []
        self.datasets = None 

    def execute(self):
        try:
            self.logger.write_line("Iniciando proceso ETL...")

            # Fase de extracción
            self.datasets = self.extract.execute()
            self.history.append(self.extract)

            # Fase de transformación
            self.output_dataset = self.transform.execute(self.datasets)
            self.history.append(self.transform) 

            # Fase de carga
            self.load.execute(self.output_dataset)
            self.history.append(self.load)

            self.logger.write_line("Proceso ETL completado.")
            
        except Exception as e:
            self.logger.write_line(f"Error durante el proceso ETL: {e}")
            self.undo()

    def undo(self):
        self.logger.write_line("Deshaciendo los cambios...")
        for command in reversed(self.history):
            command.undo()
