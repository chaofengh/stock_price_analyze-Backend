import os

import finnhub
from dotenv import load_dotenv

from utils.ttl_cache import TTLCache

load_dotenv()

alpha_vantage_api_key = os.environ.get("alpha_vantage_api_key")
finnhub_api_key = os.environ.get("finnhub_api_key")
finnhub_client = finnhub.Client(api_key=finnhub_api_key)

_FINANCIALS_CACHE = TTLCache(ttl_seconds=60 * 60 * 6, max_size=512)
_FINANCIALS_EMPTY_CACHE = TTLCache(ttl_seconds=60 * 5, max_size=512)
_NO_DATA = object()
