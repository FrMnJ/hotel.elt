import pandas as pd
import os

from etl.command import Command

class CSVLoadCommand(Command):
    def __init__(self, logger, file_path):
        super().__init__(logger)
        self.file_path = file_path

    def execute(self, data: pd.DataFrame):
        try:
            data.to_csv(self.file_path)
        except Exception as e:
            self.logger.write_line(e)
            self.undo()
    
    def undo(self):
        if os.file.exists(self.file_path):
            os.remove(self.file_path)
