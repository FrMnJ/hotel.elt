from abc import abstractmethod
from abc import ABCMeta

class Command(metaclass=ABCMeta):
    def __init__(self, logger):
        self.logger = logger

    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def undo(self):
        pass