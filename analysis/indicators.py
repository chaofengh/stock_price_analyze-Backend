import talib
import pandas as pd

def compute_bollinger_bands(data: pd.DataFrame, timeperiod=20, nbdevup=2, nbdevdn=2) -> pd.DataFrame:
    """
    Computes Bollinger Bands for the closing prices.
    """
    upper, mid, lower = talib.BBANDS(
        data['close'].astype(float),
        timeperiod=timeperiod, nbdevup=nbdevup, nbdevdn=nbdevdn, matype=0
    )
    data['BB_upper'] = upper
    data['BB_middle'] = mid
    data['BB_lower'] = lower
    return data

def compute_rsi(data: pd.DataFrame, timeperiod=6) -> pd.DataFrame:
    """
    Computes RSI for the closing prices.
    """
    data['rsi'] = talib.RSI(data['close'], timeperiod=timeperiod)
    return data
