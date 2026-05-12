from pathlib import Path

import analysis.trade_entry_evaluation as tee


def test_trade_entry_evaluation_is_split_package_with_legacy_api():
    package_dir = Path(tee.__file__).parent
    legacy_file = package_dir.with_suffix(".py")

    assert package_dir.is_dir()
    assert not legacy_file.exists()
    assert callable(tee.build_entry_decision_from_frame)
    assert callable(tee.build_entry_decision_from_context)
    assert callable(tee.run_decision_backtest)
    assert isinstance(tee._MODEL_FEATURES, list)


def test_trade_entry_evaluation_modules_stay_bounded():
    package_dir = Path(tee.__file__).parent
    modules = [path for path in package_dir.glob("*.py") if path.name != "__init__.py"]

    assert len(modules) >= 8
    line_counts = {path.name: len(path.read_text().splitlines()) for path in modules}
    assert max(line_counts.values()) <= 1200, line_counts
