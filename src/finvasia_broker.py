"""
module
"""
from typing import Any
import os
import sys
import urllib.parse
import json
import requests
import pyotp
from util import sha256
from datetime import datetime, time
from urllib.parse import urlparse, parse_qs
import pandas as pd
from pandas import DataFrame
from broker import Broker
import logging
from io import StringIO
import websocket
import threading
import time as ttime
import tomli
import zipfile

logger = logging.getLogger("algo")

BASE_URL = "https://api.shoonya.com"

WEBSOCKET_URL = "wss://api.shoonya.com/NorenWSTP/"

# NSE - Capital Market
NSE_MASTER_URL = BASE_URL + "/NSE_symbols.txt.zip"
# NSE - Equity Derivatives
NFO_MASTER_URL = BASE_URL + "/NFO_symbols.txt.zip"
# NSE - Currency Derivatives
CDS_MASTER_URL = BASE_URL + "/CDS_symbols.txt.zip"
# MCX - Commodity
MCX_MASTER_URL = BASE_URL + "/MCX_symbols.txt.zip"
# BSE - Capital Market
BSE_MASTER_URL = BASE_URL + "/BSE_symbols.txt.zip"
# BSE - Equity Derivative Segment
BFO_MASTER_URL = BASE_URL + "/BFO_symbols.txt.zip"


class FinvasiaBroker(Broker):
    """
    Broker class
    """
    def __init__(self, user_id: str, password: str, totp_secret: str, imei: str, vendor_code: str, api_key: str) -> None:
        super().__init__()
        self.user_id = user_id
        self.password = password
        self.totp_secret = totp_secret
        self.imei = imei
        self.vendor_code = vendor_code
        self.api_key = api_key
        self.is_logged_in_and_init = False

    def is_ready(self):
        return self.is_logged_in_and_init

    def login(self) -> tuple[bool, str]:
        """ login """
        totp = pyotp.TOTP(self.totp_secret).now()
        app_key = sha256(self.user_id + "|" + self.api_key)
        url = BASE_URL + "/NorenWClientTP/QuickAuth"
        passwd_sha = sha256(self.password)
        postdata = {"apkversion": "1.0.0",
                "uid": self.user_id,
                "pwd": passwd_sha,
                "factor2": totp,
                "vc": self.vendor_code,
                "appkey": app_key,
                "imei": self.imei,
                "source": "API",
                }
        payload = "jData=" + json.dumps(postdata)
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=payload)
        # print(response)
        # print(response.text)
        if response.status_code == 200:
            data = json.loads(response.text)
            if data['stat'] == 'Ok':
                token = data['susertoken']
                return True, token
            else:
                logger.info("failed to login with message")
                print(data['emsg'])
                return False, data['emsg']
        else:
            logger.info("failed to login")
            logger.error(response.text)
            return False, response.text


    def set_session_token(self, session_token: str) -> None:
        """ set sesion token """
        self.session_token = session_token

    def get_exchange_code(self, code: str) -> str:
        """
            returns exchange code used by this broker
        """
        if code == Broker.EXCHANGE_NSE:
            return "NSE"
        elif code == Broker.EXCHANGE_NFO:
            return "NFO"
        return code

    def get_nse_token(self, symbol: str) -> Any:
        """ return token """
        try:
            symbol = symbol.upper()
            if not self.nse_tokens_df.loc[(self.nse_tokens_df['Symbol'] == symbol) & (self.nse_tokens_df['Instrument'] == 'EQ')].empty:
                return self.nse_tokens_df.loc[(self.nse_tokens_df['Symbol'] == symbol) & (self.nse_tokens_df['Instrument'] == 'EQ')]['Token'].iloc[0].item()
            return None
        except Exception as e:
            logger.exception(e)


    def get_nfo_token(self, symbol, exp_date, opt_type, strike):
        """
            symbol: RELIANCE
            exp_date: 28-MAR-2024
            opt_type: CE
            srike: 900
            return token
        """
        try:
            symbol = symbol.upper()
            result = self.nfo_tokens_df.loc[(self.nfo_tokens_df['Symbol'] == symbol) &
                                    (self.nfo_tokens_df['Expiry'] == exp_date) &
                                    (self.nfo_tokens_df['OptionType'] == opt_type) &
                                    (self.nfo_tokens_df['StrikePrice'] == strike)]
            if result.empty:
                return None
            else:
                return result['Token'].iloc[0]
        except Exception as e:
            logger.exception(e)

    def get_order_history(self, order_number) -> bool:
        """ place order """

        try:
            url = BASE_URL + "/NorenWClientTP/SingleOrdHist"

            data = {"uid": self.user_id,
                    "norenordno": order_number
                    }
            payload = "jData=" + json.dumps(data) + "&jKey=" + self.session_token
            headers = {'Content-Type': 'application/json'}
            response = requests.post(url, headers=headers, data=payload)
            if response.status_code == 200:
                logger.debug(response.text)
                return response.json()
            else:

                return None
        except Exception as e:
            logger.exception(e)
            return None

    def place_order(self, symbol: str, qty: int, tr_type: str):
        """ place order """
        try:
            url = BASE_URL + "/NorenWClientTP/PlaceOrder"
            enc_scrip = urllib.parse.quote(symbol + "-EQ")

            data = {"uid": self.user_id,
                    "actid": self.user_id,
                    "exch": "NSE",
                    "tsym": enc_scrip,
                    "qty": str(qty),
                    "prc": str(0),
                    "dscqty": str(0),
                    "prd": "I",
                    "trantype": tr_type,
                    "prctyp": "MKT",
                    "ret": "DAY"
                    }
            # want to refactor this shit, later
            payload = "jData=" + json.dumps(data) + "&jKey=" + self.session_token
            headers = {'Content-Type': 'application/json'}
            response = requests.post(url, headers=headers, data=payload)
            if response.status_code == 200:
                order_resp = response.json()
                logger.debug(f"order resp: {order_resp}")
                if order_resp['stat'] == 'Ok':
                    order_number = order_resp['norenordno']
                    retryTimes = 3
                    while retryTimes > 0:
                        retryTimes -= 1
                        order_det = self.get_order_history(order_number)
                        if order_det is not None:
                            order_det = order_det[0]
                            if order_det['stat'] == 'Ok':
                                logger.debug(f"order details: {order_det}")
                                if order_det['status'] == 'REJECTED':
                                    logger.debug("order rejected")
                                    return False, 0
                                elif order_det['status'] == 'COMPLETE':
                                    logger.debug("order success")
                                    return True, float(order_det['avgprc'])
                            ttime.sleep(1)
                        else:
                            logger.debug("order details is none")
            return False, 0
        except Exception as e:
            logger.exception(e)
        return False, 0


    def buy(self, symbol: str, qty: int):
        logger.debug(f"buy order {symbol} {qty}")
        return self.place_order(symbol, qty, "B")


    def sell(self, symbol: str, qty: int):
        logger.debug(f"sell order {symbol} {qty}")
        return self.place_order(symbol, qty, "S")


    def get_last_download_date(self, key) -> str:
        if key in self.db:
            return self.db[key]
        return None


    def _download_token(self, key, zipfilename, url, outfile) -> bool:
        """ download tokens """
        try:
            response = requests.get(url, timeout=1000)

            with open(zipfilename, 'wb') as file:
                file.write(response.content)

            with zipfile.ZipFile(zipfilename, 'r') as zip_ref:
                zip_ref.extractall()

                return True
        except Exception as e:
            logger.error("error downloading token")
            logger.exception(e)


    def download_nse_tokens(self) -> None:
        self._download_token('last_download_date_nse', 'NSE_symbols.txt.zip', NSE_MASTER_URL, 'NSE_symbols.txt')


    def download_nfo_tokens(self) -> None:
        """ download tokens """
        self._download_token('last_download_date_nfo', 'NFO_symbols.txt.zip', NFO_MASTER_URL, 'NFO_symbols.txt')


    def load_tokens(self) -> None:
        self.nse_tokens_df = pd.read_csv("NSE_symbols.txt")
        self.nfo_tokens_df = pd.read_csv("NFO_symbols.txt")


    def _get_ohlc_df(self, df: DataFrame, resample: bool = True, resample_tf: str = '30T') -> DataFrame:
        df['time'] = pd.to_datetime(df['time'], dayfirst=True)
        df = df.set_index('time')
        df = df.sort_index()
        df = df.between_time('9:15', '15:30')
        df = df.rename(columns={'into': 'open'})
        df = df.rename(columns={'inth': 'high'})
        df = df.rename(columns={'intl': 'low'})
        df = df.rename(columns={'intc': 'close'})
        df = df.drop(['intvwap', 'intv', 'intoi', 'v', 'oi', 'stat', 'ssboe'], axis=1)
        if resample:
            df_resample = df.resample(resample_tf, origin='start').agg({'open': 'first',
                                                                        'high': 'max',
                                                                        'low': 'min',
                                                                        'close': 'last'})
            df_resample.dropna(inplace=True)
            return df_resample
        else:
            return df

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

        try:
            url = BASE_URL + "/NorenWClientTP/TPSeries"
            start_time = str(int(start_date.timestamp()))
            end_time = str(int(end_date.timestamp()))

            exch = self.get_exchange_code(exchange)
            token = self.get_nse_token(trading_symbol)

            postdata = {"uid": self.user_id,
                    "token": str(token),
                    "exch": exch,
                    "st": start_time,
                    "et": end_time,
                    "intrv": "1"}

            payload = "jData=" + json.dumps(postdata) + "&jKey=" + self.session_token
            headers = {'Content-Type': 'application/json'}
            response = requests.post(url, headers=headers, data=payload)
            if response.status_code == 200:
                # print(response.text)
                df = pd.read_json(StringIO(response.text))
                df = self._get_ohlc_df(df, True, timeframe)
                return df
            else:
                if response.status_code != 504:
                    logger.debug(f"response text: {response.text}")
                logger.debug(f"status code: {response.status_code}")
                return None
        except Exception as e:
            logger.exception(e)

    def on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        # print(message)
        obj = json.loads(message)
        # logger.info(obj)

        if obj['t'] == 'ck':
            if obj['s'] == 'OK':
                logger.debug('connection acknowledged')

                # Subscribe to Data
                data = {"t": "t",
                        "k": '#'.join([f'NSE|{key}' for key, value in self.tick_listeners.items()]),
                        "susertoken": self.session_token }
                payload = json.dumps(data)
                ws.send(payload)

                # Subscribe to Order updates
                data = {"t": "o",
                        "actid": self.user_id,
                        "susertoken": self.session_token}
                payload = json.dumps(data)
                ws.send(payload)

        elif obj['t'] == 'tk':
            logger.debug('subscription acknowledged')

        elif obj['t'] == 'tf':
            if "lp" in obj:
                tkn = int(obj['tk'])
                ltp = float(obj['lp'])
                t = datetime.now()
                if tkn in self.tick_listeners:
                    for strategy in self.tick_listeners[tkn]:
                        strategy.tick(t, ltp, tkn)
            else:
                pass
                # logger.debug(obj)

        elif obj['t'] == 'om':
            logger.debug("order update")
            logger.debug(obj)
            pass

        else:
            logger.debug("else")
            logger.debug(message)


    def on_error(self, ws: websocket.WebSocketApp, error: str) -> None:
        logger.debug("websocket on_error:")
        logger.debug(error)

    def on_close(self, ws: websocket.WebSocketApp, close_status_code: Any, close_msg: Any) -> None:
        logger.debug("websocket closed")


    def on_open(self, ws: websocket.WebSocketApp) -> None:
        logger.info("websocket connection opened")
        data = {"uid": self.user_id,
                "t": "c",
                "actid": self.user_id,
                "source": "API",
                "susertoken": self.session_token,
                }
        payload = json.dumps(data)
        ws.send(payload)


    def start_websocket(self) -> None:
        websocket.enableTrace(False)
        ws = websocket.WebSocketApp(WEBSOCKET_URL,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        self.ws = ws
        ws.run_forever(reconnect=5)  # Set dispatcher to automatic reconnection, 5 second reconnect delay if connection closed unexpectedly
        # rel.signal(2, rel.abort)  # Keyboard Interrupt
        # rel.dispatch()
        # ws.run_forever()

    def resubscribe(self):
        # Subscribe to Data
        data = {"t": "t",
                "k": '#'.join([f'NSE|{key}' for key, value in self.tick_listeners.items()]),
                "susertoken": self.session_token }
        payload = json.dumps(data)
        self.ws.send(payload)

    def start_websocket_thread(self):
        thread = threading.Thread(target = self.start_websocket)
        thread.start()

    def login_and_init(self):

        if self.is_logged_in_and_init:
            raise Exception("Already Logged In Init")

        login_success, session_token = self.login()
        if not login_success:    # try again
            ttime.sleep(5)
            login_success, session_token = self.login()
        if not login_success:    # try again
            ttime.sleep(5)
            login_success, session_token = self.login()
        if login_success:
            logger.debug(f"session token: {session_token}")
            self.set_session_token(session_token)
            self.download_nse_tokens()
            self.download_nfo_tokens()
            self.load_tokens()
            self.is_logged_in_and_init = True
            return True
        return False

if __name__ == '__main__':
    logger = logging.getLogger("algo")
    s_handler = logging.StreamHandler(sys.stdout)
    s_handler.setLevel(logging.DEBUG)
    s_format = logging.Formatter('%(message)s')
    s_handler.setFormatter(s_format)

    logger.addHandler(s_handler)
    logger.setLevel(logging.DEBUG)

    config = []

    with open("config.toml", mode="rb") as fp:
        config = tomli.load(fp)
        # print(json.dumps(config, indent=2))
        account = next((item for item in config["accounts"] if item.get("name") == "my-finvasia-account"), None)

    user_id = account['user_id']
    password = account['password']
    totp_token = account['totp_token']
    imei = account['imei']
    vendor_code = account['vendor_code']
    api_key = account['api_key']

    broker = FinvasiaBroker(user_id, password, totp_token, imei, vendor_code, api_key)
    status, token = broker.login()
    if status:
        broker.set_session_token(token)
        logger.info("logged in successfully")

    broker.login_and_init()
    start_date = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
    end_date = datetime.now().replace(hour=15, minute=30, second=0, microsecond=0)
    df = broker.get_ohlc("NSE", "SAIL", start_date, end_date, "1T")
    print(df)
