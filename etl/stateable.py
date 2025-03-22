from abc import ABCMeta, abstractmethod

class Stateable(metaclass=ABCMeta):
    @abstractmethod
    def get_state(self):
        pass

    @abstractmethod
    def set_state(self, state):
        pass

