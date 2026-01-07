# tests/test_data_fetcher_exports.py

import analysis.data_fetcher as data_fetcher


def test_data_fetcher_exports():
    for name in data_fetcher.__all__:
        assert hasattr(data_fetcher, name)
