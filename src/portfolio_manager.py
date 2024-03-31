import logging

logger = logging.getLogger("algo")

class PortfolioManager:
    def __init__(self, max_open: int, max_trades: int, max_sl_hits: int) -> None:
        self.max_open = max_open
        self.max_trades = max_trades
        self.max_sl_hits = max_sl_hits

        self.open_positions = 0
        self.trades = 0
        self.sl_hits = 0

        self.total_profit = 0
        self.current_profit = 0
        self.target_achieved = 0

        # logger.debug(f"init: max open: {max_open}, max trades: {max_trades}, max sl hits: {max_sl_hits}")

    def can_take_entry(self) -> bool:
        logger.debug("can take entry")
        logger.debug(f"open_positions: {self.open_positions}, trades: {self.trades}, sl_hits: {self.sl_hits}, total_profit: {self.total_profit}, current_profit: {self.current_profit}")

        logger.debug(f"self.open_positions < self.max_open: {self.open_positions < self.max_open}")
        logger.debug(f"self.trades < self.max_trades:       {self.trades < self.max_trades}")
        logger.debug(f"self.sl_hits < self.max_sl_hits:     {self.sl_hits < self.max_sl_hits}")

        result = self.open_positions < self.max_open and self.trades < self.max_trades and self.sl_hits < self.max_sl_hits
        logger.debug(f"result: {result}")
        return result

    def entered(self) -> None:
        logger.debug("entered")
        self.open_positions += 1
        self.trades += 1
        logger.debug(f"open_positions: {self.open_positions}, trades: {self.trades}, sl_hits: {self.sl_hits}, total_profit: {self.total_profit}, current_profit: {self.current_profit}")

    def exited(self, profit: float, sl_hit: bool) -> None:
        logger.debug("exited")
        self.open_positions -= 1
        self.total_profit += profit
        if sl_hit:
            self.sl_hits += 1
        logger.debug(f"open_positions: {self.open_positions}, trades: {self.trades}, sl_hits: {self.sl_hits}, total_profit: {self.total_profit}, current_profit: {self.current_profit}")

    def update_profit(self, profit: float) -> None:
        logger.debug("update profit")
        self.current_profit += profit
        logger.debug(f"open_positions: {self.open_positions}, trades: {self.trades}, sl_hits: {self.sl_hits}, total_profit: {self.total_profit}, current_profit: {self.current_profit}")




