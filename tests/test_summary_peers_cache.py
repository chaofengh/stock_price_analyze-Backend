from analysis import summary_peers


def test_get_summary_bundle_reuses_cached_subresults(monkeypatch):
    summary_peers._clear_summary_caches_for_tests()

    monkeypatch.setattr(summary_peers, "_SUMMARY_TTL_SECONDS", 60)
    monkeypatch.setattr(summary_peers, "_PEERS_TTL_SECONDS", 60)
    monkeypatch.setattr(summary_peers, "_FUNDAMENTALS_TTL_SECONDS", 60)
    monkeypatch.setattr(summary_peers, "_PEER_INFO_TTL_SECONDS", 60)
    monkeypatch.setattr(summary_peers, "_PEER_AVG_TTL_SECONDS", 60)

    calls = {
        "summary": 0,
        "peers": 0,
        "fundamentals": 0,
        "peer_info": 0,
        "peer_avgs": 0,
    }

    def fake_summary(symbol: str):
        calls["summary"] += 1
        return {
            "symbol": symbol,
            "chart_data": [],
            "window_5": {},
            "window_10": {},
            "aggregated_window_5": {},
            "aggregated_window_10": {},
            "avg_consecutive_touch_days": {
                "1M": {"upper": None, "lower": None},
                "3M": {"upper": None, "lower": None},
                "YTD": {"upper": None, "lower": None},
                "1Y": {"upper": None, "lower": None},
            },
        }

    def fake_peers(symbol: str):
        calls["peers"] += 1
        return ["BBB", "CCC"]

    def fake_fundamentals(symbol: str, include_alpha: bool = True):
        calls["fundamentals"] += 1
        return {"trailingPE": 12.5, "beta": 1.1}

    def fake_peer_info(peers: list):
        calls["peer_info"] += 1
        return {
            peer: {"latest_price": 10.0, "percentage_change": 1.0, "intraday_close_5m": []}
            for peer in peers
        }

    def fake_peer_avgs(peers: list, max_workers=None):
        calls["peer_avgs"] += 1
        return {
            "avg_peer_trailingPE": 9.5,
            "avg_peer_forwardPE": 8.5,
            "avg_peer_PEG": 1.2,
            "avg_peer_PGI": 0.9,
            "avg_peer_beta": 1.0,
        }

    monkeypatch.setattr(summary_peers, "get_summary", fake_summary)
    monkeypatch.setattr(summary_peers, "get_peers", fake_peers)
    monkeypatch.setattr(summary_peers, "get_fundamentals", fake_fundamentals)
    monkeypatch.setattr(summary_peers, "get_peer_info", fake_peer_info)
    monkeypatch.setattr(summary_peers, "get_peer_metric_averages", fake_peer_avgs)

    first = summary_peers.get_summary_bundle("AAA")
    second = summary_peers.get_summary_bundle("AAA")

    assert first["symbol"] == "AAA"
    assert second["symbol"] == "AAA"
    assert calls["summary"] == 1
    assert calls["peers"] == 1
    assert calls["fundamentals"] == 1
    assert calls["peer_info"] == 1
    assert calls["peer_avgs"] == 1

    first["peer_info"]["BBB"]["latest_price"] = 999.0
    third = summary_peers.get_summary_bundle("AAA")
    assert third["peer_info"]["BBB"]["latest_price"] == 10.0
