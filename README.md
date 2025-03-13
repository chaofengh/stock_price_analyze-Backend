# Stock Analysis Backend

## Overview
This backend application is built using Flask and provides a suite of REST API endpoints for a stock analysis web app. It integrates with external data sources (Yahoo Finance, Alpha Vantage, Finnhub) to fetch stock and options data, performs technical analysis (Bollinger Bands, RSI, etc.), and offers real-time alerts via Server-Sent Events (SSE). The application also schedules a daily scan job using APScheduler to update alerts.

## Features
- **REST API Endpoints:** 
  - Stock summary analysis
  - Alerts (real-time streaming and latest results)
  - Ticker management (GET, POST, DELETE)
  - Option price ratio computation
- **Scheduled Tasks:** Runs a daily scan at 4:30 PM on weekdays (with an optional immediate run during development).
- **Technical Analysis:** Calculates Bollinger Bands, RSI, and additional metrics.
- **Database Integration:** Manages ticker symbols in a PostgreSQL database.
- **Peer Comparison:** Fetches and compares stock fundamentals from external APIs.

## Directory Structure

## Prerequisites
- **Python 3.x**
- **PostgreSQL** database
- Required Python packages (see `requirements.txt`):
  - Flask
  - Flask-CORS
  - python-dotenv
  - APScheduler
  - pandas
  - numpy
  - psycopg2
  - yfinance
  - finnhub-python
  - requests
  - TA-Lib
  - Alpha Vantage SDK (if applicable)

## Installation

1. **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd <repository_folder>
    ```

2. **Create and activate a virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3. **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4. **Configure environment variables:**
    Create a `.env` file in the root directory with the following variables:
    ```env
    front_end_client_website=http://your-frontend-domain.com
    DATABASE_URL=postgresql://username:password@hostname:port/dbname
    alpha_vantage_api_key=your_alpha_vantage_api_key
    finnhub_api_key=your_finnhub_api_key
    ```

5. **Set up the database:**
    Run the script to create the tickers table in your PostgreSQL database:
    ```bash
    python creatre_ticker_table.py
    ```

## Running the Application

To start the backend in development mode, run your main application file (e.g., `main_application.py`):

```bash
python main_application.py
API Endpoints
Alerts
GET /api/alerts/stream
Streams the latest scan result via Server-Sent Events (SSE).

GET /api/alerts/latest
Retrieves the latest scan result as JSON.

Summary
GET /api/summary?symbol=<SYMBOL>
Returns the analysis summary for the provided stock symbol.
Tickers
GET /api/tickers
Retrieves intraday data for all tickers in the database.

POST /api/tickers
Adds new tickers. Accepts payloads like:Option Price Ratio
GET /api/option-price-ratio
Computes and returns the best out-of-the-money put option for each ticker along with relevant ratios and error messages if applicable.
Scheduled Tasks
Daily Scan:
A background job that:
Fetches ticker data from the database.
Prepares and analyzes the stock data.
Detects Bollinger Band touches.
Updates a global variable (latest_scan_result) used for alerts.
This job is scheduled to run every weekday at 4:30 PM via APScheduler.
Additional Functionality
Technical Indicators:
Utilizes TA-Lib to compute Bollinger Bands and RSI.

Data Preparation & Event Detection:
Prepares stock data for analysis and identifies price events such as touches and hugs.

Peer Analysis:
Compares a stock's fundamentals against its peers by fetching data from external APIs.

Chart Data Builder:
Prepares data formatted for front-end visualization.

Contributing
Contributions are welcome! Please fork the repository and submit a pull request with your improvements.

License
This project is licensed under the MIT License.

Contact
For questions or support, please contact [cfhuang001@gmail.com].
