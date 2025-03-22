from abc import ABCMeta, abstractmethod

class ETLFactory(metaclass=ABCMeta):
    @abstractmethod
    def get_extract(self):
        pass
    @abstractmethod
    def get_transform(self):
        pass
    @abstractmethod
    def get_load(self):
        pass