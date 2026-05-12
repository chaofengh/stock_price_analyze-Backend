"""Entry-decision model package.

Public imports are kept compatible with the historical
``analysis.trade_entry_evaluation`` module while the implementation is split by
responsibility across smaller modules.
"""
from __future__ import annotations

from .settings import *
from .features import *
from .model import *
from .playbooks import *
from .adaptive import *
from .decision import *
from .backtest import *
from .quality import *
from .payload import *

__all__ = [name for name in globals() if not name.startswith("__")]
