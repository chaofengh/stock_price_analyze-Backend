from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analysis.trade_entry_evaluation import get_entry_decision  # noqa: E402


METRIC_KEYS = (
    "eligible_touch_count",
    "raw_prediction_count",
    "raw_accuracy",
    "raw_continue_accuracy",
    "raw_reverse_accuracy",
    "raw_expected_return",
    "raw_expected_atr_return",
    "prediction_count",
    "accuracy",
    "coverage",
)


def _num(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if out == out else default


def _summarize_symbol(symbol: str) -> dict:
    payload = get_entry_decision(symbol)
    summary: dict[str, dict] = {}
    for horizon_key, backtest in (payload.get("backtest_1y") or {}).items():
        if not isinstance(backtest, dict):
            continue
        summary[horizon_key] = {key: backtest.get(key) for key in METRIC_KEYS}
    return summary


def _aggregate(results: dict[str, dict]) -> dict:
    totals: dict[str, dict] = {}
    for symbol_result in results.values():
        if not isinstance(symbol_result, dict) or "error" in symbol_result:
            continue
        for horizon_key, item in symbol_result.items():
            if not isinstance(item, dict):
                continue
            bucket = totals.setdefault(
                horizon_key,
                {
                    "raw_prediction_count": 0,
                    "raw_correct_count": 0,
                    "prediction_count": 0,
                    "correct_count": 0,
                },
            )
            raw_count = int(_num(item.get("raw_prediction_count")))
            deployed_count = int(_num(item.get("prediction_count")))
            bucket["raw_prediction_count"] += raw_count
            bucket["raw_correct_count"] += int(round(_num(item.get("raw_accuracy")) * raw_count))
            bucket["prediction_count"] += deployed_count
            bucket["correct_count"] += int(round(_num(item.get("accuracy")) * deployed_count))

    for item in totals.values():
        raw_count = item["raw_prediction_count"]
        deployed_count = item["prediction_count"]
        item["raw_accuracy"] = round(item["raw_correct_count"] / raw_count, 6) if raw_count else None
        item["accuracy"] = round(item["correct_count"] / deployed_count, 6) if deployed_count else None
    return totals


def _print_table(results: dict[str, dict], aggregate: dict) -> None:
    print("symbol horizon raw_n raw_acc deployed_n deployed_acc coverage")
    for symbol, symbol_result in results.items():
        if "error" in symbol_result:
            print(f"{symbol} error {symbol_result['error']}")
            continue
        for horizon_key in sorted(symbol_result):
            item = symbol_result[horizon_key]
            print(
                symbol,
                horizon_key,
                item.get("raw_prediction_count"),
                item.get("raw_accuracy"),
                item.get("prediction_count"),
                item.get("accuracy"),
                item.get("coverage"),
            )
    for horizon_key in sorted(aggregate):
        item = aggregate[horizon_key]
        print(
            "TOTAL",
            horizon_key,
            item.get("raw_prediction_count"),
            item.get("raw_accuracy"),
            item.get("prediction_count"),
            item.get("accuracy"),
            "",
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare entry-decision walk-forward accuracy by symbol.")
    parser.add_argument("--symbols", nargs="+", default=["MSFT", "AAPL", "NVDA"])
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = parser.parse_args()

    results: dict[str, dict] = {}
    for symbol in args.symbols:
        normalized = symbol.strip().upper()
        if not normalized:
            continue
        try:
            results[normalized] = _summarize_symbol(normalized)
        except Exception as exc:
            results[normalized] = {"error": str(exc)}

    aggregate = _aggregate(results)
    payload = {"symbols": results, "aggregate": aggregate}
    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        _print_table(results, aggregate)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
