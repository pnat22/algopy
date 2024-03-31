"""
utility functions
"""

import hashlib
from datetime import datetime, timedelta, time

MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY = range(7)

def sha256(text: str) -> str:
    """ returs hash """
    return hashlib.sha256(text.encode()).hexdigest()

def get_holiday_list() -> list[str]:
    """ returns holiday list of string """
    with open("holidays.txt", "r") as f:
        content = f.read()
        dates = filter(lambda x: x.lstrip().rstrip() != '', content.split("\n"))
        return list(dates)

def get_today() -> datetime:
    """ return today date """
    return datetime.today()

def get_prev_trading_day(for_date: datetime = None) -> datetime:
    """ get previous trading day, considering holidays and weekends """
    if for_date is None:
        for_date = datetime.today()
    holidays = get_holiday_list()
    while True:
        for_date = for_date - timedelta(days=1)
        if for_date.weekday() == SATURDAY or for_date.weekday() == SUNDAY:
            continue
        if for_date.date().isoformat() in holidays:
            continue
        return datetime.combine(for_date, time.min)
        # return for_date
        # dt = datetime.datetime.fromordinal()

def percentage_change(old_value, new_value):
    # Ensure old_value is not zero to avoid division by zero
    if old_value != 0:
        percentage_change = ((new_value - old_value) / abs(old_value)) * 100
        return percentage_change
    else:
        # Handle the case where old_value is zero to avoid division by zero
        return None