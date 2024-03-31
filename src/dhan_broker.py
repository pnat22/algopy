"""
module
"""
from typing import Any
import pandas as pd
from broker import Broker
import logging
from sqlitedict import SqliteDict
from dhanhq import dhanhq
import time as ttime

logger = logging.getLogger("breakout")

class DhanBroker(Broker):
    """
    Broker class
    """
    def __init__(self, client_id: str, access_token: str) -> None:
        super().__init__()
        self.client_id = client_id
        self.access_token = access_token
        self.db = SqliteDict("db.sqlite")
        self.dhan = dhanhq(self.client_id, self.access_token)
        self.is_logged_in_and_init = False

    def is_ready(self):
        return self.is_logged_in_and_init

    def load_tokens(self) -> None:
        dtypes={'SEM_CUSTOM_SYMBOL': 'str',
               'SEM_STRIKE_PRICE': 'str',
               'SEM_TICK_SIZE': 'str',
               'SEM_EXCH_INSTRUMENT_TYPE': 'str',
               'SEM_SERIES': 'str',
               'SEM_SMST_SECURITY_ID': 'str'}
        self.df = pd.read_csv("api-scrip-master-eq.csv", dtype=dtypes)

    def get_token(self, symbol: str) -> Any:
        """ return token """
        return symbol

    def download_tokens(self) -> None:
        pass

    def get_security_id(self, exch: str, instrument_name: str, symbol: str) -> str:
        """
            exch: BSE, NSE, MCX
            instrument_name: EQUITY, FUTCUR, OPTCUR, FUTIDX, etc.,
            symbol: HDFCBANK, etc.,
        """
        # print(self.df.loc[(self.df['SEM_EXM_EXCH_ID'] == exch) & (self.df['SEM_INSTRUMENT_NAME'] == instrument_name) & (self.df['SEM_TRADING_SYMBOL'] == symbol)].head())
        id = self.df.loc[(self.df['SEM_EXM_EXCH_ID'] == exch) &
                    (self.df['SEM_INSTRUMENT_NAME'] == instrument_name) &
                    (self.df['SEM_TRADING_SYMBOL'] == symbol)]
        if id.shape[0] == 1:
            return id.iloc[0]['SEM_SMST_SECURITY_ID']
        return None

    def login(self, uid: str, passwd: str) -> tuple[bool, str]:
        pass

    def place_order(self, trading_symbol: str, qty: int, tr_type: str) -> bool:
        """ place order
            tr_type: 'B' or 'S'
        """
        security_id = self.get_security_id(Broker.EXCHANGE_NSE, "EQUITY", trading_symbol)
        transaction_type = self.dhan.BUY if tr_type == 'B' else self.dhan.SELL
        print(f"placing order security_id: {security_id}, trasaction_type:{transaction_type}")
        order_resp = self.dhan.place_order(security_id=security_id,
            exchange_segment=self.dhan.NSE,
            transaction_type=transaction_type,
            quantity=qty,
            order_type=self.dhan.MARKET,
            product_type=self.dhan.INTRA,
            price=0)
        logger.info(f"order resp: {order_resp}")
        print(f"order resp: {order_resp}")
        if order_resp['status'] == 'success':
            order_id = order_resp['data']['orderId']
            retryTimes = 3
            while retryTimes > 0:
                retryTimes -= 1
                order_det = self.dhan.get_order_by_id(order_id)
                print(order_det)

                logger.info(f"order details: {order_det}")
                if order_det['data']['orderStatus'] == 'REJECTED':
                    logger.debug("order rejected")
                    return False
                elif order_det['data']['orderStatus'] == 'TRADED':
                    logger.debug("order success")
                    return True
                ttime.sleep(1)
        return False

    def buy(self, trading_symbol: str, qty: int) -> bool:
        """
            buy order will be placed
        """
        logger.info(f"buy order {trading_symbol} {qty}")
        return self.place_order(trading_symbol, qty, "B")


    def sell(self, trading_symbol: str, qty: int) -> bool:
        """
            sell order will be placed
        """
        logger.info(f"sell order {trading_symbol} {qty}")
        return self.place_order(trading_symbol, qty, "S")

    def get_ohlc(self,
                 security_id: str,
                 segment: str,
                 instr_type: str,
                 start_date: str,
                 end_date: str) -> Any:
        """
            exchange_segment: NSE_EQ NSE_FNO NSE_CURRENCY BSE_EQ MCX_COMM IDX_I
            instr_type: EQUITY FUTCOM FUTCUR FUTIDX FUTSTK INDEX OPTSTK OPTIDX
        """
        result = self.dhan.historical_minute_charts(security_id, segment, instr_type, None, start_date, end_date)
        print(result)


    def login_and_init(self):
        self.load_tokens()
        self.is_logged_in_and_init = True
        return True


if __name__ == '__main__':
    client_id = ""
    access_token = ""
    broker = DhanBroker(client_id, access_token)
    resp = broker.sell("LTTS", 16)
    print(resp)

