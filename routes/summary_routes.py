import re
from datetime import datetime

from flask import Blueprint, jsonify, request

from analysis.summary import (
    get_summary,
    get_summary_bundle,
    get_summary_overview,
    get_summary_peers,
    get_summary_fundamentals,
    get_summary_peer_averages,
)
from tasks.entry_decision_preload_tasks import (
    get_preloaded_entry_decision,
    refresh_entry_decision_preload_state,
    request_full_entry_decision_preload,
)
from tasks.entry_signal_tasks import safe_sync_entry_signals_from_payload
from utils.serialization import convert_to_python_types

summary_blueprint = Blueprint("summary", __name__)

_TEMPORARY_PRELOAD_REASONS = {
    "interactive_symbol_preload_backoff",
    "symbol_preload_backoff",
    "preload_source_backoff",
    "preload_start_failed",
}


def _get_symbol() -> str:
    symbol = request.args.get("symbol", default="QQQ")
    return symbol.strip().upper()


def _normalize_request_as_of_date(as_of_date: str | None) -> str | None:
    if as_of_date is None:
        return None
    text = str(as_of_date).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) is None:
        raise ValueError(f"Invalid as_of_date '{as_of_date}'. Expected YYYY-MM-DD.")
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid as_of_date '{as_of_date}'. Expected YYYY-MM-DD.") from None
    return text


def _entry_decision_loading_payload(symbol: str, as_of_date: str | None, preload_result: dict | None) -> dict:
    retry_after = 2
    if isinstance(preload_result, dict):
        try:
            retry_after = int(float(preload_result.get("retry_after_seconds") or retry_after))
        except (TypeError, ValueError):
            retry_after = 2
    return {
        "status": "loading",
        "symbol": symbol,
        "requested_as_of_date": as_of_date,
        "retry_after_seconds": max(1, min(retry_after, 30)),
        "preload": preload_result or {"status": "started"},
    }


def _entry_decision_loading_response(symbol: str, as_of_date: str | None, preload_result: dict | None):
    payload = _entry_decision_loading_payload(symbol, as_of_date, preload_result)
    response = jsonify(payload)
    response.status_code = 202
    response.headers["Retry-After"] = str(payload["retry_after_seconds"])
    return response


@summary_blueprint.route("/api/summary", methods=["GET"])
def summary_endpoint():
    """
    Example usage:
      GET /api/summary?symbol=QQQ
    Returns the analysis summary as JSON.
    """
    symbol = _get_symbol()
    try:
        payload = convert_to_python_types(get_summary(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@summary_blueprint.route("/api/summary/overview", methods=["GET"])
def summary_overview_endpoint():
    """
    Lightweight overview: fundamentals, peer averages, and peer info.
    Example usage:
      GET /api/summary/overview?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        overview = convert_to_python_types(get_summary_overview(symbol))
        return jsonify(overview), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@summary_blueprint.route("/api/summary/peers", methods=["GET"])
def summary_peers_endpoint():
    """
    Lightweight peer info for People Also View.
    Example usage:
      GET /api/summary/peers?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        payload = convert_to_python_types(get_summary_peers(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@summary_blueprint.route("/api/summary/fundamentals", methods=["GET"])
def summary_fundamentals_endpoint():
    """
    Lightweight fundamentals for a single ticker.
    Example usage:
      GET /api/summary/fundamentals?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        payload = convert_to_python_types(get_summary_fundamentals(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@summary_blueprint.route("/api/summary/peer-averages", methods=["GET"])
def summary_peer_averages_endpoint():
    """
    Peer averages for valuation metrics.
    Example usage:
      GET /api/summary/peer-averages?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        payload = convert_to_python_types(get_summary_peer_averages(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@summary_blueprint.route("/api/summary/bundle", methods=["GET"])
def summary_bundle_endpoint():
    """
    Full summary payload (chart + fundamentals + peers + peer averages).
    Example usage:
      GET /api/summary/bundle?symbol=QQQ
    """
    symbol = _get_symbol()
    try:
        payload = convert_to_python_types(get_summary_bundle(symbol))
        return jsonify(payload), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@summary_blueprint.route("/api/summary/entry-decision", methods=["GET"])
def summary_entry_decision_endpoint():
    """
    Two-stage Bollinger decision object + 1Y prediction-accuracy backtest.
    Example usage:
      GET /api/summary/entry-decision?symbol=QQQ&as_of_date=2026-04-15
    """
    symbol = _get_symbol()
    as_of_date = request.args.get("as_of_date", default=None, type=str)
    try:
        if not symbol:
            raise ValueError("symbol is required.")
        as_of_date = _normalize_request_as_of_date(as_of_date)
        refresh_entry_decision_preload_state()
        payload = get_preloaded_entry_decision(symbol, as_of_date=as_of_date, full_only=True)
        if payload is None:
            preload_result = request_full_entry_decision_preload(
                symbol,
                as_of_date=as_of_date,
                force=True,
                ignore_backoff=True,
            )
            payload = get_preloaded_entry_decision(symbol, as_of_date=as_of_date, full_only=True)
            if payload is None:
                if isinstance(preload_result, dict) and preload_result.get("reason") in _TEMPORARY_PRELOAD_REASONS:
                    preload_result = {**preload_result, "status": "loading"}
                return _entry_decision_loading_response(symbol, as_of_date, preload_result)
        safe_sync_entry_signals_from_payload(symbol, payload, source="entry_decision_api")
        return jsonify(payload), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
