"""
broker
"""
from typing import Any
import abc
from data_provider import DataProvider
from tick_listener import TickListener

class Broker(DataProvider, abc.ABC):
    """ abstract class broker """

    EXCHANGE_NSE: str="NSE"
    EXCHANGE_NFO: str="NFO"

    @abc.abstractmethod
    def buy(self, symbol: str, qty: int) -> bool:
        """ buy """

    @abc.abstractmethod
    def sell(self, symbol: str, qty: int) -> bool:
        """ sell """

    def __init__(self):
        self.tick_listeners = {}

    def subscribe(self, token, tick_listener):
        if token not in self.tick_listeners:
            self.tick_listeners[token] = []

        self.tick_listeners[token].append(tick_listener)


class DummyBroker(Broker):
    """
        used for backtesting
    """
    def __init__(self) -> None:
        pass

    def buy(self, symbol: str, qty: int) -> None:
        """ buy """
        print(f"buy order {symbol} {qty}")

    def sell(self, symbol: str, qty: int) -> None:
        """ sell """
        print(f"sell order {symbol} {qty}")
