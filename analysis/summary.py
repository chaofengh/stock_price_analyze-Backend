"""
summary.py
Facade for summary builders (kept for backwards-compatible imports).
"""
from .summary_core import get_summary
from .summary_peers import (
    get_summary_bundle,
    get_summary_overview,
    get_summary_peers,
    get_summary_fundamentals,
    get_summary_peer_averages,
)
from .trade_entry_evaluation import get_entry_decision

__all__ = [
    "get_summary",
    "get_summary_bundle",
    "get_summary_overview",
    "get_summary_peers",
    "get_summary_fundamentals",
    "get_summary_peer_averages",
    "get_entry_decision",
]
