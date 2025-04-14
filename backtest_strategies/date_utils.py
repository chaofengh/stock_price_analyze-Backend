# backtest_strategies/date_utils.py

import datetime
import pytz

def is_us_eastern_dst(date_val):
    dt_utc = datetime.datetime(date_val.year, date_val.month, date_val.day, tzinfo=pytz.UTC)
    dt_eastern = dt_utc.astimezone(pytz.timezone("US/Eastern"))
    return bool(dt_eastern.dst())
