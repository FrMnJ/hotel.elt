from command import Command
from etl.etl_factory import ETLFactory

class ETL(Command):
    def __init__(self, logger, etl_factory: ETLFactory):
        super().__init__(logger)
        self.extract = etl_factory.get_extract()
        self.transform = etl_factory.get_transform()
        self.load = etl_factory.get_load()
        self.history = []
    
    def execute(self):
        try:
            self.extract.execute()
            self.history.append(self.extract)
            self.transform.execute()
            self.history.append(self.transform)
            self.load.execute()
            self.history.append(self.load)
        except Exception as e:
            self.logger.write_line(e)
            self.undo()
            self.load_state()
    
    def undo(self):
        for command in reversed(self.history):
            command.undo()