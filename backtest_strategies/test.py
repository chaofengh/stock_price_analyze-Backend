# backtest_strategies/test.py

# fallback so this file can be run both as:
#   1) python3 -m backtest_strategies.test    (package mode)
#   2) python3 test.py                        (script mode)
try:
    # when run as a package
    from .runner import run_backtest_grid
except ImportError:
    # when run directly as a script
    from runner import run_backtest_grid

from pandas.testing import assert_frame_equal
import pandas as pd

if __name__ == "__main__":
    payload = run_backtest_grid("QQQ", top_n=20)

    s1, s2 = payload["scenarios"][0], payload["scenarios"][1]

    assert_frame_equal(
        pd.DataFrame(s1["daily_trades"]).sort_index(axis=1),
        pd.DataFrame(s2["daily_trades"]).sort_index(axis=1),
        check_like=True,
    )
    print("✓ No difference in trade logs.")
