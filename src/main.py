from typing import Any, List
import sys
import logging
import json
import os
from datetime import datetime, time, timedelta
from time import sleep, localtime
import urllib.parse
from io import StringIO
import pandas as pd
from pandas import DataFrame
import pandas_ta as ta
import requests
import websocket
from sqlitedict import SqliteDict
import tomli
from breakout_strategy import BreakoutStrategy
from concurrent.futures import ThreadPoolExecutor

from broker import Broker
from ft_broker import FlatTradeBroker
from zebu_broker import ZebuBroker
from dhan_broker import DhanBroker
from finvasia_broker import FinvasiaBroker
from data_provider import DataProvider

from portfolio_manager import PortfolioManager
from broker import DummyBroker
from util import get_prev_trading_day
# from nseindia import NseIndia

NO_POS = 0
LONG_POS = 1
SHORT_POS = -1


db = SqliteDict("db.sqlite")

logger = logging.getLogger("algo")

s_handler = logging.StreamHandler(sys.stdout)
f_handler = logging.FileHandler('algo.log')
s_handler.setLevel(logging.DEBUG)
f_handler.setLevel(logging.DEBUG)

s_format = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d - %(message)s')
f_format = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d - %(message)s')

s_handler.setFormatter(s_format)
f_handler.setFormatter(f_format)

logger.addHandler(s_handler)
logger.addHandler(f_handler)
logger.setLevel(logging.DEBUG)

logging.getLogger("websocket").setLevel(logging.WARNING)


def save_df_to_file(foldername, symbol, df):
    fname = ""
    if not os.path.exists(foldername):
        os.makedirs(foldername)
    datetimestr = ""
    timestr = datetime.now().strftime("%Y-%m-%d-%H-%M")
    df.to_csv(f"{foldername}/{symbol}-{timestr}.csv")


def get_account_by_name(accounts, value):
    key_to_search = 'name'
    value_to_search = value

    # Find the dictionary with the specified key-value pair
    found_dict = next((item for item in accounts if item.get(key_to_search) == value_to_search), None)

    if found_dict is not None:
        return found_dict
    else:
        return None

def get_broker(config) -> Broker:
    if config['broker'] == 'zebu':
        broker: ZebuBroker = ZebuBroker(config['user_id'], config['password'], config['secret'], config['api_key'])
        broker.login_and_init()
        return broker
    elif config['broker'] == 'flat':
        broker: FlatTradeBroker = FlatTradeBroker(config['user_id'], config['password'], config['api_key'], config['api_secret'], config['totp'])
        broker.login_and_init()
        return broker
    elif config['broker'] == 'dhan':
        broker: DhanBroker = DhanBroker(config['client_id'], config['access_token'])
        broker.login_and_init()
        return broker
    elif config['broker'] == 'finvasia':
        user_id = config['user_id']
        password = config['password']
        totp_token = config['totp_token']
        imei = config['imei']
        vendor_code = config['vendor_code']
        api_key = config['api_key']
        broker: FinvasiaBroker = FinvasiaBroker(user_id, password, totp_token, imei, vendor_code, api_key)
        broker.login_and_init()
        return broker
    return None

def main() -> None:
    """"
    main function
    """
    config = {}
    with open("config.toml", mode="rb") as fp:
        config = tomli.load(fp)
        print(json.dumps(config, indent=2))

    if config['strategy']['name'] == 's1':
        pass
    elif config['strategy']['name'] == 's2':
        pass
    elif config['strategy']['name'] == 'BreakoutStrategy':
        main_BreakoutStrategy(config)

def main_BreakoutStrategy(config):
    cash_to_trade = config['strategy']['cash_to_trade']
    portfolio = PortfolioManager(1, 100, 10)
    data_account_config = get_account_by_name(config['accounts'], config['strategy']['data_account_name'])
    trade_account_config = get_account_by_name(config['accounts'], config['strategy']['trading_account_name'])

    data_broker = get_broker(data_account_config)
    if data_account_config['name'] != trade_account_config['name']:
        trade_broker = get_broker(trade_account_config)
    else:
        trade_broker = data_broker

    if data_broker.is_ready() and trade_broker.is_ready():
        start_breakout_strategy(portfolio, trade_broker, data_broker, cash_to_trade, config['strategy']['stocks'])
    else:
        print("broker not ready")

def get_fetch_fno_data(broker, start_date, end_date):
    def fetch_fno_data(symbol):
        retries = 3
        while retries > 0:
            retries -= 1
            try:
                df = broker.get_ohlc("NSE", symbol, start_date, end_date, "1T")
                if df is not None:
                    current_time = (datetime.now())
                    rounded_time = current_time - timedelta(minutes=current_time.minute % 5, seconds=current_time.second, microseconds=current_time.microsecond)
                    rounded_time = rounded_time - timedelta(microseconds=1)
                    formatted_time = rounded_time.strftime('%H:%M:%S')
                    logger.debug(f"{datetime.now().strftime('%d %B, %Y %A %H:%M:%S')}, formatted time: {formatted_time}")
                    df = df.between_time(start_date.strftime('%H:%M:%S'), formatted_time)
                    save_df_to_file("newhigh-data-5min", symbol, df)
                    return df
            except Exception as e:
                print(e)
                logger.info(f"An error occurred: {e}")
    return fetch_fno_data

def get_fnolist(dataFrames):
    data = []
    for symbol, df in dataFrames.items():
        open_price = df.iloc[0].open
        close_price = df.iloc[-1].close
        pctChange = ((close_price - open_price) / open_price) * 100
        data.append({'symbol': symbol, 'pctChange': pctChange })
    df = pd.DataFrame(data)
    up = len(df[df['pctChange'] > 0])
    down = len(df[df['pctChange'] < 0])
    df = df.sort_values(by=['pctChange'], ascending=down > up)
    logger.debug("new fno list")
    logger.debug(df)
    lst = (df['symbol'].tolist())
    return lst

def start_breakout_strategy(portfolio: PortfolioManager, trade_broker: Broker, data_broker: Broker, cash_to_trade: float, scrips) -> None:
    strategies = {}

    start_date = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
    end_date = datetime.now().replace(hour=15, minute=30, second=0, microsecond=0)

    counter = 2
    for scrip in scrips:
        logger.info(f"{scrip}")
        strategies[scrip] = BreakoutStrategy(scrip, trade_broker, data_broker, portfolio, cash_to_trade)
        counter += 1

    data_broker.start_websocket_thread()

    day_close_time = time(hour=15, minute=35)
    while True:
        if datetime.now().time() > day_close_time:
            logger.info('day close triggered')
            os._exit(0)
        now_time = localtime()
        sleep(60 - now_time.tm_sec)
        tme = (datetime.now().time())
        if tme.minute % 1 == 0 and portfolio.open_positions != portfolio.max_open and portfolio.can_take_entry():
            try:
                fetch_and_save = get_fetch_fno_data(data_broker, start_date, end_date)
                with ThreadPoolExecutor(max_workers=5) as executor:
                    results = list(executor.map(fetch_and_save, scrips))
                    dataframes = {}
                    for index, df in enumerate(results):
                        dataframes[scrips[index]] = df
                    for symbol in scrips:
                        strategy = strategies[symbol]
                        try:
                            strategy.on_1min_close(datetime.now(), dataframes[symbol])
                        except Exception as e:
                            logger.exception(e)
            except Exception as e:
                logger.exception(f"An error occurred: {e}")

def save_pid():
    current_pid = os.getpid()

    # Save the PID to a file
    pid_file_path = 'pid.txt'
    with open(pid_file_path, 'w') as pid_file:
        pid_file.write(str(current_pid))

def start():
    save_pid()

    with open('holidays.txt') as f:
        holidays = [l.strip() for l in f.readlines()]

    if datetime.today().date().isoformat() in holidays:
        print("holiday")
    else:
        main()

if __name__ == '__main__':
    logger = logging.getLogger("algo")
    s_handler = logging.StreamHandler(sys.stdout)
    s_handler.setLevel(logging.DEBUG)
    s_format = logging.Formatter('%(message)s')
    s_handler.setFormatter(s_format)

    logger.addHandler(s_handler)
    logger.setLevel(logging.DEBUG)

    start()

