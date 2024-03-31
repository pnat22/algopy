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
from sqlitedict import SqliteDict
import pandas as pd
from pandas import DataFrame
from broker import Broker
import logging
from io import StringIO
import websocket
import threading
import time as ttime
import tomli

logger = logging.getLogger("algo")

# BASE_URL = "https://auth.flattrade.in/"

# https://pidata.flattrade.in/scripmaster/json/nse
# https://pidata.flattrade.in/scripmaster/json/nfo
# https://pidata.flattrade.in/scripmaster/json/nfoidx
# https://pidata.flattrade.in/scripmaster/json/nfostk
# https://pidata.flattrade.in/scripmaster/json/bfoidx
# https://pidata.flattrade.in/scripmaster/json/bfostk
# https://pidata.flattrade.in/scripmaster/json/cds
# https://pidata.flattrade.in/scripmaster/json/mcx

class FlatTradeBroker(Broker):
    """
    Broker class
    """
    def __init__(self, user_id: str, password: str, api_key: str, api_secret: str, totp_secret: str) -> None:
        super().__init__()
        self.user_id = user_id
        self.password = password
        self.api_key = api_key
        self.api_secret = api_secret
        self.totp_secret = totp_secret
        self.is_logged_in_and_init = False

    def is_ready(self):
        return self.is_logged_in_and_init

    def set_session_token(self, session_token: str) -> None:
        """ set sesion token """
        self.session_token = session_token

    def get_user_details(user_id: str, key: str) -> tuple[bool, Any]:
        """
            get user details
        """
        url = "https://piconnect.flattrade.in/PiConnectTP/UserDetails"

        data = { "uid": user_id }

        logger.info(data)
        logger.info(json.dumps(data))

        payload = "jData=" + json.dumps(data) + "&jKey=" + key
        logger.info(payload)
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=payload, timeout=1000)
        if response.status_code == 200:
            data = json.loads(response.text)
            logger.info(response.text)
            return True, data
        else:
            return False, response.text

    def login(self) -> tuple[bool, str]:
        """ login """
        apikey = self.api_key
        secret= self.api_secret
        totp = pyotp.TOTP(self.totp_secret)

        passwd_sha = sha256(self.password)

        session_id = ""
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
            "Accept": "application/json",
            "Referer": "https://auth.flattrade.in/",
            "Origin": "https://auth.flattrade.in",
        }

        session = requests.Session()
        response = session.post(f"https://authapi.flattrade.in/auth/session", headers=headers, timeout=3000)

        if not response.text.strip() == "":
            session_id = response.text.strip()
        else:
            logger.info("session id blank")
            return (False, "session id blank")

        response = session.get(f"https://auth.flattrade.in/?app_key={apikey}")

        payload = {"UserName": self.user_id,
                "Password": passwd_sha,
                "PAN_DOB": totp.now(),
                "App":"",
                "ClientID":"",
                "Key":"",
                "APIKey":apikey,
                "Sid": session_id }

        response = session.post(f"https://authapi.flattrade.in/ftauth", headers=headers,
                                json=payload)
        logger.debug(response.text)
        data = json.loads(response.text)
        parsed_url = urlparse(data['RedirectURL'])
        query_params = parse_qs(parsed_url.query)
        code_param = query_params.get('code', None)

        if code_param is None:
            logger.error("login error: code_param is none")
            return (False, "code_param is none")

        if len(code_param) == 1:
            code_param = code_param[0]
        # logger.info('code_param:', code_param)

        url = "https://authapi.flattrade.in/trade/apitoken"

        request_code = code_param
        api_secret = sha256(apikey + request_code + secret)

        data = {"api_key": apikey,
                "request_code": request_code,
                "api_secret": api_secret }
        payload = data
        response = session.post(url, headers=headers, json=payload, timeout=1000)
        # logger.info("apitoken:", response.text)
        # logger.info(response)

        data = json.loads(response.text)
        if(data['stat']) == 'Ok':
            return (True, data['token'])
        return (False, data['emsg'])

    def get_exchange_code(self, code: str) -> str:
        """
            returns exchange code used by this broker
        """
        if code == Broker.EXCHANGE_NSE:
            return "NSE"
        elif code == Broker.EXCHANGE_NFO:
            return "NFO"
        return code

    # def _get_token(self, ft_exchange_code, trading_symbol) -> str:
    #     if ft_exchange_code == "NSE":
    #        return trading_symbol + "-EQ"
    #     elif ft_exchange_code == "NFO":
    #         return trading_symbol
    #     return trading_symbol


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
            url = "https://piconnect.flattrade.in/PiConnectTP/TPSeries"
            start_time = str(int(start_date.timestamp()))
            end_time = str(int(end_date.timestamp()))

            exch = self.get_exchange_code(exchange)
            token = self.get_token(trading_symbol)

            postdata = {"uid": self.user_id,
                    "token": token,
                    "exch": exch,
                    "st": start_time,
                    "et": end_time,
                    "intrv": "1"}
            payload = "jData=" + json.dumps(postdata) + "&jKey=" + self.session_token
            headers = {'Content-Type': 'application/json'}
            response = requests.post(url, headers=headers, data=payload)
            if response.status_code == 200:
                df = pd.read_json(StringIO(response.text))
                df = self._get_ohlc_df(df, True, timeframe)
                return df
            else:
                if response.status_code != 504:
                    logger.error(f"response text: {response.text}")
                logger.error(f"status code: {response.status_code}")
                return None
        except Exception as e:
            logger.exception(e)

    def get_order_history(self, order_number) -> bool:
        """ place order """

        try:
            url = "https://piconnect.flattrade.in/PiConnectTP/SingleOrdHist"

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
            url = "https://piconnect.flattrade.in/PiConnectTP/PlaceOrder"
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
            payload = "jData=" + json.dumps(data) + "&jKey=" + self.session_token
            headers = {'Content-Type': 'application/json'}
            response = requests.post(url, headers=headers, data=payload)
            if response.status_code == 200:
                order_resp = response.json()
                logger.debug(f"order resp: {order_resp}")
                print(f"order resp: {order_resp}")
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


    def buy(self, symbol: str, qty: int) -> bool:
        logger.debug(f"buy order {symbol} {qty}")
        return self.place_order(symbol, qty, "B")

    def sell(self, symbol: str, qty: int) -> bool:
        logger.debug(f"sell order {symbol} {qty}")
        return self.place_order(symbol, qty, "S")

    def get_last_download_date(self) -> str:
        if 'last_download_date' in self.db:
            return self.db['last_download_date']
        return ""

    def load_tokens(self) -> None:
        st = ""
        with open('nse.json', 'r') as f:
            st = f.read()
        j = json.loads(st)
        df = pd.DataFrame(j["data"])
        self.tokens = df
        # print(df)


    def get_token(self, symbol: str) -> Any:
        """ return token """
        try:
            if not self.tokens.loc[self.tokens['symbol'] == symbol, 'token'].empty:
                token = self.tokens.loc[self.tokens['symbol'] == symbol, 'token'].iloc[0]
                return int(token)
        except Exception as e:
            logger.exception(e)

    def get_nse_token(self, symbol: str) -> Any:
        """ return token """
        return self.get_token(symbol)

    def download_tokens(self) -> None:
        """ download tokens """
        try:
            filename = 'nse.json'
            response = requests.get('https://pidata.flattrade.in/scripmaster/json/nse', timeout=1000)

            with open(filename, 'w') as file:
                file.write(response.text)
        except Exception as e:
            logger.exception(e)

    def on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        # print(message)
        obj = json.loads(message)
        # logger.info(obj)

        if obj['t'] == 'ck':
            if obj['s'] == 'OK':
                logger.info('connection acknowledged')

                # Subscribe to Data
                data = {"t": "t",
                        "k": '#'.join([f'NSE|{key}' for key, value in self.tick_listeners.items()])
                }
                #       "susertoken": self.session_token }
                payload = json.dumps(data)
                print(f"subscribe payload: {payload}")
                ws.send(payload)

                # # Subscribe to Order updates
                # data = {"t": "o",
                #         "actid": self.user_id,
                #         "susertoken": self.session_token}
                # payload = json.dumps(data)
                # ws.send(payload)
        elif obj['t'] == 'tk':
            logger.debug(obj)
            logger.info('subscription acknowledged')
        elif obj['t'] == 'tf':
            if "lp" in obj:
                tkn = int(obj['tk'])
                ltp = float(obj['lp'])
                t = datetime.now()
                # print(self.tick_listeners)
                if tkn in self.tick_listeners:
                    for strategy in self.tick_listeners[tkn]:
                        strategy.tick(t, ltp)
            else:
                # logger.debug(obj)
                pass

        elif obj['t'] == 'om':
            logger.info("order update")
            logger.info(obj)
        else:
            logger.info("else")
            logger.info(message)

    def on_error(self, ws: websocket.WebSocketApp, error: str) -> None:
        print(f"websocket on_error: {error}")
        logger.exception(error)

    def on_close(self, ws: websocket.WebSocketApp, close_status_code: Any, close_msg: Any) -> None:
        print("websocket closed")
        logger.info("### closed ###")

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
        ws = websocket.WebSocketApp("wss://piconnect.flattrade.in/PiConnectWSTp/",
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        ws.run_forever(reconnect=5)  # Set dispatcher to automatic reconnection, 5 second reconnect delay if connection closed unexpectedly
        # rel.signal(2, rel.abort)  # Keyboard Interrupt
        # rel.dispatch()
        # ws.run_forever()

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
            logger.info(f"session token: {session_token}")
            self.set_session_token(session_token)
            self.db = SqliteDict("db.sqlite")
            self.download_tokens()
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

    with open("config.toml", mode="rb") as fp:
        config = tomli.load(fp)
        account = next((item for item in config["accounts"] if item.get("name") == "my-flat-account"), None)

    broker = FlatTradeBroker(account["user_id"], account["password"], account["api_key"], account["api_secret"], account["totp"])

    broker.login_and_init()
    broker.load_tokens()
    token = broker.get_nse_token('RELIANCE')
    print(token)
    result, price = broker.place_order("TATACOMM", 0, "B")
    if result:
        print(f"result: {result}, average price: {price}")
    else:
        print(f"result: {result}")

