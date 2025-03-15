# tests/test_option_price_ratio.py
import pandas as pd
from unittest.mock import patch

def test_option_price_ratio_no_tickers(client):
    """
    If the DB returns zero tickers, we should just get an empty list.
    """
    with patch("routes.Option_price_ratio_routes.get_all_tickers", return_value=[]):
        response = client.get("/api/option-price-ratio")
        assert response.status_code == 200
        data = response.get_json()
        assert data == []

def test_option_price_ratio_success(client):
    """
    Mock a scenario with a single ticker and test the best OTM put logic.
    """
    with patch("routes.Option_price_ratio_routes.get_all_tickers", return_value=["TSLA"]):
        # Create a DataFrame to simulate "option_data" correctly.
        mock_option_df = pd.DataFrame({
            "strike": [190, 195, 205],
            "lastPrice": [5.0, 7.0, 2.0]
        })
        mock_df = {
            "stock_price": 200.0,
            "option_data": mock_option_df
        }
        with patch("routes.Option_price_ratio_routes.fetch_stock_option_data", return_value=mock_df):
            response = client.get("/api/option-price-ratio")
            assert response.status_code == 200
            data = response.get_json()
            # It's a list of results, 1 item for "TSLA"
            assert len(data) == 1
            item = data[0]
            assert item["ticker"] == "TSLA"
            # The best OTM put is the one with strike < 200 and highest lastPrice, which is 7.0.
            assert item["best_put_price"] == 7.0
