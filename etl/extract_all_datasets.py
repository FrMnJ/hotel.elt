from command import Command
from extract_command import ExtractCommand
import os
import pandas as pd

class ExtractAllDatasetsCommand(Command):
    def __init__(self, logger):
        super().__init__(logger)

    def execute(self):
        self.logger.write_line('Extracting all datasets...')
        dfs = {}
        for dataset in os.listdir('./datasets'):
            self.logger.write_line(f'Extracting {dataset}...')
            extract_command = ExtractCommand(self.logger, dataset) 
            dfs[dataset] = extract_command.execute()
            self.logger.write_line(f'Extracted {dataset}')
        self.logger.write_line('Joining all datasets...')
        return dfs
    
    def undo(self):
        pass

if __name__ == "__main__":
    from debug_logger import DebugLogger as Logger
    logger = Logger()
    extract_all_datasets = ExtractAllDatasetsCommand(logger)
    dfs = extract_all_datasets.execute()
    print(dfs)

 