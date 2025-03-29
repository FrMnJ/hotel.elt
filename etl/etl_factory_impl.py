from extract_all_datasets import ExtractAllDatasetsCommand
from clean_data_command import CleanDataCommand
from csv_load_command import CSVLoadCommand
from excel_load_command import ExcelLoadCommand
from json_load_command import JsonLoadCommand
from db_load_command import DBLoadCommand
from no_command import NoCommand
from etl_factory import ETLFactory  

class ETLFactoryImpl(ETLFactory):
    def __init__(self, logger, output_format, output_destination, table_name):
        self.logger = logger
        self.output_format = output_format
        self.output_destination = output_destination
        self.table_name = table_name

    def get_extract(self):
        # Usar ExtractCommand para la fase de extracción
        return ExtractAllDatasetsCommand(self.logger)
        
    def get_transform(self):
        # Usamos CleanDataCommand para la transformación
        return CleanDataCommand(self.logger)

    def get_load(self):
        # Dependiendo del formato de salida, devolvemos el comando de carga correspondiente
        if self.output_format == "csv":
            return CSVLoadCommand(self.logger, self.output_destination)
        elif self.output_format == "xlsx":
            return ExcelLoadCommand(self.logger, self.output_destination)
        elif self.output_format == "json":
            return JsonLoadCommand(self.logger, self.output_destination)
        elif self.output_format == "sql":
            if self.output_destination and self.table_name:
                return DBLoadCommand(self.logger, self.output_destination, self.table_name)
            else:
                self.logger.write_line("Conexión a la base de datos no configurada.")
                return NoCommand()
        else:
            self.logger.write_line("Formato de salida no soportado.")
            return NoCommand()