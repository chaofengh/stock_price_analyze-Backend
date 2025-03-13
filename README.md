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
