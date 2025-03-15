import requests

packages = [
    "Flask",
    "Flask-Cors",
    "python-dotenv",
    "APScheduler",
    "pandas",
    "numpy",
    "psycopg2",
    "yfinance",
    "finnhub-python",
    "requests",
    "ta-lib",
    "alpha_vantage",
    "pytest",
]

for pkg in packages:
    url = f"https://pypi.org/pypi/{pkg}/json"
    response = requests.get(url)
    data = response.json()
    latest_version = data["info"]["version"]
    print(f"{pkg}: {latest_version}")
