from flask import Blueprint, request, jsonify
from analysis.data_fetcher import fetch_stock_option_data  
from database.ticker_repository import get_all_tickers
import datetime

option_price_ratio_blueprint = Blueprint('option_price_ratio', __name__)

@option_price_ratio_blueprint.route('/api/option-price-ratio', methods=['GET'])
def get_option_price_ratio():
    """
    GET endpoint to fetch the out-of-the-money put option with the highest price
    for all tickers in the database.
    
    Expiration date is automatically set to:
      - The upcoming Friday if today is Monday-Thursday.
      - Next week's Friday if today is Friday, Saturday, or Sunday.
    
    Returns:
      A JSON list of objects, each representing one ticker. Each object has:
        - ticker
        - expiration
        - stock_price
        - best_put_option (details about the best OTM put)
        - best_put_price
        - best_put_ratio (best_put_price / stock_price)
        - error (only if something failed for that ticker)
    """
    try:
        # 1) Retrieve all tickers from the database
        tickers = get_all_tickers()

        # 2) Compute expiration date: upcoming Friday or next week's Friday
        today = datetime.date.today()
        if today.weekday() < 4:  # Monday (0) to Thursday (3)
            days_until_friday = 4 - today.weekday()
        else:  # Friday (4), Saturday (5), or Sunday (6)
            days_until_friday = 11 - today.weekday()
        expiration = (today + datetime.timedelta(days=days_until_friday)).strftime("%Y-%m-%d")

        results = []

        # 3) Loop over each ticker and fetch its best OTM put option
        for ticker in tickers:
            try:
                fetch_result = fetch_stock_option_data(
                    ticker=ticker,
                    expiration=expiration,
                    all_expirations=False,
                    option_type="puts"
                )

                stock_price = fetch_result.get("stock_price")
                option_data = fetch_result.get("option_data")

                if stock_price is None:
                    results.append({
                        "ticker": ticker,
                        "expiration": expiration,
                        "error": "Could not retrieve the latest trading price for the stock."
                    })
                    continue

                if option_data is None or option_data.empty:
                    results.append({
                        "ticker": ticker,
                        "expiration": expiration,
                        "error": "No puts data found for the given expiration."
                    })
                    continue

                # Filter for out-of-the-money put options (strike < stock_price)
                otm_puts = option_data[option_data['strike'] < stock_price]
                if otm_puts.empty:
                    results.append({
                        "ticker": ticker,
                        "expiration": expiration,
                        "error": "No out-of-the-money puts found."
                    })
                    continue

                # Select the put option with the highest 'lastPrice'
                best_idx = otm_puts['lastPrice'].idxmax()
                best_row = otm_puts.loc[best_idx].to_dict()
                best_price = best_row.get('lastPrice')
                best_ratio = best_price / stock_price if stock_price else None

                # 4) Add the result for this ticker
                results.append({
                    "ticker": ticker,
                    "expiration": expiration,
                    "stock_price": stock_price,
                    "best_put_option": best_row,
                    "best_put_price": best_price,
                    "best_put_ratio": best_ratio
                })

            except Exception as ticker_error:
                # If something failed for a particular ticker, record the error
                results.append({
                    "ticker": ticker,
                    "expiration": expiration,
                    "error": str(ticker_error)
                })

        # 5) Return a list of results, one entry per ticker
        return jsonify(results), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500