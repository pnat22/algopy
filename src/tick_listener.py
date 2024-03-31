import abc
from datetime import datetime, time

class TickListener(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def tick(self, dtime: datetime, ltp: float, token: int) -> None:
        raise NotImplementedError

