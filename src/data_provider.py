"""
data provider module
"""
from typing import Any
import logging
from datetime import datetime
import abc
logger = logging.getLogger("breakout")

class DataProvider(abc.ABC):
    """
     class
    """
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    def get_ohlc(self, exchange: str,
                 trading_symbol: str,
                 start_date: datetime,
                 end_date: datetime,
                 timeframe: str) -> Any:
        """
            params:
                exch: NSE,
                timeframe: '1T', '5T', ...
            get ohlc as pandas dataframe
            index -> pandas timestamp
            columns, open, high, low, close
        """
        pass
