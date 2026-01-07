# tests/test_data_fetcher_context.py

from utils.ttl_cache import TTLCache

import analysis.data_fetcher_context as context


def test_context_initializes_caches_and_client():
    assert isinstance(context._FINANCIALS_CACHE, TTLCache)
    assert isinstance(context._FINANCIALS_EMPTY_CACHE, TTLCache)
    assert context._NO_DATA is not None
    assert hasattr(context.finnhub_client, "company_peers")
