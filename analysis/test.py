from .data_preparation import prepare_stock_data, get_trading_period
def get_summary(symbol: str) -> dict:

    # 1. Fetch and prepare data
    data_dict = prepare_stock_data(symbol)
    print(data_dict)
    if symbol not in data_dict:
        raise ValueError(f"No data found for symbol {symbol}")
    df = data_dict[symbol]
    return df

get_summary('AAPL')