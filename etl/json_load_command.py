import pandas as pd
from command import Command
import os

class JsonLoad(Command):
    def __init__(self, file_path, logger):
        super().__init__(logger)
        self.file_path = file_path

    def execute(self, data: pd.DataFrame):
        try: 
            data.to_json(self.file_path)
        except Exception as e:
            self.logger.write_line(e)
            self.undo()

    def undo(self):
        if os.file.exists(self.file_path):
            os.remove(self.file_path)
