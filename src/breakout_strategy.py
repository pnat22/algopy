"""
breakout strategy

"""

from datetime import datetime, time, timedelta
from data_provider import DataProvider
from portfolio_manager import PortfolioManager
from broker import Broker
from pandas import DataFrame
import logging
import sys
from util import get_prev_trading_day, percentage_change
import time as ttime
import os
import pandas_ta as ta
import sys
import pandas as pd

logger = logging.getLogger("algo")

NO_POS = 0
LONG_POS = 1
SHORT_POS = -1

class BreakoutStrategy:
    """ breakout strategy """
    def __init__(self, symbol: str, trade_broker: Broker, data_broker: Broker, portfolio: PortfolioManager, cash_to_trade: float) -> None:
        """
            cash_to_trade = cash allocated to trade this strategy, for ex. if 10,000 is cash_to_trade - qty to trade will be (cash_to_trade / ltp)
        """
        self.symbol = symbol
        self.broker = trade_broker
        self.portfolio = portfolio
        self.cash_to_trade = cash_to_trade

        self.tick_df = pd.DataFrame(columns=['time', 'price'])
        self.tick_df.set_index('time', inplace=True)

        self.token = data_broker.get_nse_token(self.symbol)
        # print(f"symbol: {self.symbol} token: {self.token}")
        data_broker.subscribe(self.token, self)

        self.nr_trades = 0
        self.nr_reversal = 0
        self.no_more_trades = False

        self.order_pending = False
        self.position = NO_POS

        # self.strategy_start_time = time(hour=9, minute=30)
        self.breakout_range_time = time(hour=9, minute=20)
        self.eod_exit_time = time(hour=15, minute=10, second=0)
        self.max_entry_allowed_time = time(hour=14, minute=55)

        self.buy_price: float = 0.0
        self.sell_price: float = 0.0
        self.entry_time = None
        self.exit_time = None

        self.ltp: float = 1.0
        self.capital = 0
        self.traded_qty = 0

        self.breakout_calculated = False
        self.breakout_high: float = float('-inf')
        self.breakout_low: float = float('inf')
        self.day_high: float = float('-inf')
        self.day_low: float = float('inf')
        self.breakout_range: 0

        self.sl_percent = 0.95
        self.sl_shift_after_percent = 0.8
        self.target1_percent = 2.0
        self.target2_percent = 25
        self.trailing = False
        self.trail_sl_percent = 0.75
        self.current_target_percent = self.target1_percent

        self.sl_shift_percent_on_slhit = 0.35
        self.breakout_plus_percent = 0.65

        self.breakout_shift_percent = 0.01
        self.stop_loss = 0
        self.risk_percent = 1
        self.profit = 0
        self.prev_ltp = 0
        self.sl_shifted = False
        self.target3_percent = 2.5

        self.target_percents  = [1.9, 5.0]
        self.exitqty_percents = [0.75, 0.25]

        self.slhit_count = 0
        self.max_slhit = 3
        self.target_count = 0
        self.max_target = 15

        self.breakout_calculated = False

    def day_close(self) -> None:
        """ called at eod """
        if self.position == LONG_POS:
            logger.info(f'end of day exit position, ltp:{self.ltp}')
            self.broker.sell(self.symbol, self.traded_qty)
            self.sell_price = self.ltp
            self.position = NO_POS
        elif self.position == SHORT_POS:
            logger.info(f'end of day exit position, ltp:{self.ltp}')
            self.broker.buy(self.symbol, self.traded_qty)
            self.buy_price = self.ltp
            self.position = NO_POS

    def on_1min_close(self, dtime: datetime, df: DataFrame) -> None:
        """ on bar close """
        logger.debug(f"on 1 min close: {dtime}")
        # logger.debug(self.symbol)
        # logger.debug(dtime)
        # logger.debug(df)
        if dtime.time() >= self.breakout_range_time and self.breakout_calculated == False:
            # logger.debug("breakout time")
            start_time = pd.to_datetime('1900-01-01 09:15').time()
            end_time = pd.to_datetime('1900-01-01 ' + self.breakout_range_time.strftime('%H:%M')).time()
            try:
                filtered_df = df[(df.index.time >= start_time) & (df.index.time < end_time)]
                self.breakout_high, self.breakout_low = filtered_df['high'].max(), filtered_df['low'].min()
                logger.info(f"high: {self.breakout_high}, low: {self.breakout_low}")
                self.breakout_high += self.breakout_high * self.breakout_plus_percent / 100.0
                self.breakout_low  -= self.breakout_low  * self.breakout_plus_percent / 100.0
                logger.info(f"shifted high: {self.breakout_high}, shifted low: {self.breakout_low}")
                self.breakout_calculated = True
            except Exception as e:
                logger.exception(e)

        ltp = df.iloc[-1]['close'].item()


    def get_qty_by_risk(self, capital: float, sell_price: float, buy_price: float, risk_percent: float) -> int:
        """
            calculates qty to trade for the given risk
        """
        qty = (capital * risk_percent) / (abs(sell_price - buy_price) * 100)
        return int(qty)

    def save_tick_data(self, dtime: datetime, ltp: float) -> None:
        self.tick_df.loc[dtime.time()] = ltp
        if self.tick_df.shape[0] % 30 == 0:
            if os.path.exists("tickdata") == False:
                os.mkdir("tickdata")
            datestr = datetime.now().strftime("%Y-%m-%d")
            filename = f"tickdata/{self.symbol}-{datestr}.csv"
            self.tick_df.to_csv(filename)


    def tick(self, dtime: datetime, ltp: float, token: int) -> None:
        # tick
        self.save_tick_data(dtime, ltp)
        self.tick_time = dtime.time()
        self.ltp = ltp

        if self.slhit_count == self.max_slhit:
            self.no_more_trades = True
            return

        if self.breakout_calculated == False:
            return

        self.day_low = min(self.day_low, ltp)
        self.day_high = max(self.day_high, ltp)

        if self.trailing:
            if self.position == LONG_POS:
                self.stop_loss = self.day_high - (self.day_high * self.trail_sl_percent / 100)
            elif self.position == SHORT_POS:
                self.stop_loss = self.day_low + (self.day_low * self.trail_sl_percent / 100)

        if self.position == NO_POS and self.tick_time < self.max_entry_allowed_time and \
            self.no_more_trades == False and self.slhit_count < self.max_slhit and \
            (self.target_count == self.max_target and self.profit > 0) == False:
            self.check_to_enter(dtime, ltp)
        else:
            self.check_to_exit(dtime, ltp)

        self.prev_ltp = ltp

    def check_to_enter(self, dtime: datetime, ltp: float) -> None:
        # logger.debug("checking to enter position")
        buy_condition = ltp > self.breakout_high
        sell_condition = ltp < self.breakout_low
        if buy_condition:
            if True:
                self.traded_qty = self.cash_to_trade // ltp
                result, price = self.broker.buy(self.symbol, self.traded_qty)
                if result:
                    self.portfolio.entered()
                    self.buy_price = price
                    self.position = LONG_POS
                    self.nr_trades += 1
                    self.entry_time = self.tick_time
                    self.stop_loss = self.ltp - (self.ltp * self.sl_percent / 100.0)
                    self.sl_shifted = False
                    self.trailing = False
                    self.current_target_percent = self.target1_percent
                    target = price + (price * self.current_target_percent / 100)
                    logger.info(f'buy triggered at: {self.tick_time}, {self.symbol}, {self.symbol}, ltp:{self.ltp}, qty:{self.traded_qty}, stop_loss: {self.stop_loss:.2f}, target: {target}')
                else:
                    self.no_more_trades = True
                    logger.info(f"entry rejected, no more trades for {self.symbol}")

        elif sell_condition:
            if True:
                self.traded_qty = self.cash_to_trade // ltp
                result, price = self.broker.sell(self.symbol, self.traded_qty)
                if result:
                    self.portfolio.entered()
                    self.sell_price = price
                    self.position = SHORT_POS
                    self.nr_trades += 1
                    self.entry_time = self.tick_time
                    self.stop_loss = self.ltp + (self.ltp * self.sl_percent / 100.0)
                    self.sl_shifted = False
                    self.trailing = False
                    self.current_target_percent = self.target1_percent
                    target = price - (price * self.current_target_percent / 100)
                    logger.info(f'sell triggered at: {self.tick_time}, {self.symbol}, ltp:{self.ltp}, qty:{self.traded_qty}, stop_loss: {self.stop_loss:.2f}, target: {target}')
                else:
                    self.no_more_trades = True
                    logger.info(f"entry rejected, no more trades for {self.symbol}")


    def check_to_exit(self, dtime: datetime, ltp: float) -> None:
        if self.position == LONG_POS:
            pc = abs(percentage_change(self.buy_price, ltp))
            if pc > self.sl_shift_after_percent and ltp > self.buy_price and self.sl_shifted == False:
                self.stop_loss = self.buy_price + (self.buy_price * 0.05 / 100)
                self.sl_shifted = True
                logger.info(f"{self.tick_time} stop loss shifted to buy price, {self.symbol} new sl: {self.stop_loss:.2f}")

            if ltp < self.stop_loss:
                result, price = self.broker.sell(self.symbol, self.traded_qty)
                if result:
                    cur_profit = (ltp - self.buy_price) * self.traded_qty
                    self.profit = self.profit + cur_profit
                    self.portfolio.exited(cur_profit, self.sl_shifted == False)
                    self.position = NO_POS
                    if self.sl_shifted == False:
                        self.slhit_count += 1
                    self.breakout_high = self.breakout_high + (self.breakout_high * self.sl_shift_percent_on_slhit / 100)
                    logger.info(f'sl triggered at: {self.tick_time}, {self.symbol}, ltp: {self.ltp}, breakout_high: {self.breakout_high}, qty: {self.traded_qty}, cur profit: {cur_profit:.2f}, totalmtm: {self.profit:.2f}')
                    # self.breakout_high = self.day_high
                    # logger.info(f"shifting breakout high to day high: {self.breakout_high}")

            elif ltp > self.buy_price + (self.buy_price * self.current_target_percent / 100.0):
                exit_qty = int(self.traded_qty * 60 / 100)
                result, price = self.broker.sell(self.symbol, exit_qty)
                if result:
                    self.traded_qty -= exit_qty
                    cur_profit = (ltp - self.buy_price) * exit_qty
                    self.profit = self.profit + cur_profit
                    self.portfolio.exited(cur_profit, False)
                    # self.position = NO_POS
                    # self.no_more_trades = True
                    self.target_count += 1
                    self.current_target_percent = self.target2_percent
                    self.stop_loss = self.ltp - (self.ltp * self.trail_sl_percent / 100.0)
                    self.trailing = True
                    self.breakout_high = self.day_high
                    logger.info(f'target achieved at: {self.tick_time}, {self.symbol}, ltp: {self.ltp}, exit_qty: {exit_qty}, cur profit: {cur_profit:.2f}, totalmtm: {self.profit:.2f}')

        elif self.position == SHORT_POS:
            pc = abs(percentage_change(self.sell_price, ltp))
            if pc > self.sl_shift_after_percent and ltp < self.sell_price and self.sl_shifted == False:
                self.stop_loss = self.sell_price - (self.sell_price * 0.05 / 100)
                self.sl_shifted = True
                logger.info(f"{self.tick_time}, stop loss shifted to sell price, {self.symbol} new sl: {self.stop_loss} ltp: {ltp}")

            if ltp > self.stop_loss:
                result, price = self.broker.buy(self.symbol, self.traded_qty)
                if result:
                    cur_profit = (self.sell_price - ltp) * self.traded_qty
                    self.profit = self.profit + cur_profit
                    self.portfolio.exited(cur_profit, self.sl_shifted == False)
                    self.position = NO_POS
                    if self.sl_shifted == False:
                        self.slhit_count += 1
                    self.breakout_low = self.breakout_low - (self.breakout_low * self.sl_shift_percent_on_slhit / 100)
                    logger.info(f'sl triggered at: {self.tick_time}, {self.symbol}, ltp: {self.ltp}, breakout_low: {self.breakout_low}, qty: {self.traded_qty}, cur profit: {cur_profit:.2f}, totalmtm: {self.profit:.2f}')
                    # logger.info(f"shifting breakout low to: {self.breakout_low}")


            elif ltp < self.sell_price - (self.sell_price * self.current_target_percent / 100.0):
                exit_qty = int(self.traded_qty * 60 / 100)
                result, price = self.broker.buy(self.symbol, exit_qty)
                if result:
                    self.traded_qty -= exit_qty
                    cur_profit = (self.sell_price - ltp) * exit_qty
                    self.profit = self.profit + cur_profit
                    self.portfolio.exited(cur_profit, True)
                    # self.position = NO_POS
                    self.target_count += 1
                    self.stop_loss = self.ltp + (self.ltp * self.trail_sl_percent / 100.0)
                    self.current_target_percent = self.target2_percent
                    self.trailing = True
                    self.breakout_low = self.day_low
                    logger.info(f'target achieved at: {self.tick_time}, {self.symbol}, ltp: {self.ltp}, exit_qty: {exit_qty}, cur profit: {cur_profit:.2f}, totalmtm: {self.profit:.2f}')


        # if not exited already check eod exit
        if self.position != NO_POS:
            self.close_eod_positions()

    def close_eod_positions(self):
        if self.tick_time >= self.eod_exit_time:
            if self.position == LONG_POS:
                result, price =  self.broker.sell(self.symbol, self.traded_qty)
                if result:
                    self.sell_price = self.ltp
                    self.position = NO_POS
                    self.no_more_trades = True
                    cur_profit = (self.sell_price - self.buy_price) * self.traded_qty
                    self.profit = self.profit + cur_profit
                    logger.info(f'end of day exit position, {self.tick_time}, {self.symbol}, ltp:{self.ltp}, cur profit: {cur_profit:.2f}, totalmtm: {self.profit:.2f}')
                    self.portfolio.exited(self.profit, True)
                else:
                    logger.error(f'sell failed: {self.symbol}')

            elif self.position == SHORT_POS:
                result, price =  self.broker.buy(self.symbol, self.traded_qty)
                if result:
                    self.buy_price = self.ltp
                    self.position = NO_POS
                    self.no_more_trades = True
                    cur_profit = (self.sell_price - self.buy_price) * self.traded_qty
                    self.profit = self.profit + cur_profit
                    logger.info(f'end of day exit position, {self.tick_time}, {self.symbol},  ltp:{self.ltp}, profit: {cur_profit:.2f}, totalmtm: {self.profit:.2f}')
                    self.portfolio.exited(self.profit, True)
                else:
                    logger.error(f'buy failed: {self.symbol}')



