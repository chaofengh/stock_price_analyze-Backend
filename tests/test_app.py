# tests/test_app.py
from unittest.mock import patch

from app import create_app, create_scheduler


class _FakeScheduler:
    def __init__(self):
        self.jobs = []
        self.listeners = []
        self.started = False

    def add_job(self, func, **kwargs):
        self.jobs.append({"func": func, **kwargs})

    def add_listener(self, listener, mask):
        self.listeners.append((listener, mask))

    def start(self):
        self.started = True

    def shutdown(self, wait=False):  # pragma: no cover - called by registered callback
        return wait


def test_app_creation(app):
    assert app is not None
    assert app.testing is True


def test_create_scheduler_registers_half_hour_scan_and_watchlist_jobs():
    flask_app = create_app(testing=True)
    fake_scheduler = _FakeScheduler()

    with (
        patch("app.BackgroundScheduler", return_value=fake_scheduler) as mock_scheduler_cls,
        patch("app.prime_scan_cache") as mock_prime_scan,
        patch("app.atexit.register") as mock_atexit_register,
    ):
        scheduler = create_scheduler(flask_app)

    assert scheduler is fake_scheduler
    mock_scheduler_cls.assert_called_once()
    mock_prime_scan.assert_called_once()
    mock_atexit_register.assert_called_once()
    assert fake_scheduler.started is True

    scheduler_kwargs = mock_scheduler_cls.call_args.kwargs
    assert scheduler_kwargs["job_defaults"]["misfire_grace_time"] == 600
    assert scheduler_kwargs["job_defaults"]["coalesce"] is True
    assert scheduler_kwargs["job_defaults"]["max_instances"] == 1

    daily_job = next(job for job in fake_scheduler.jobs if job["id"] == "daily_scan")
    assert daily_job["trigger"] == "cron"
    assert daily_job["day_of_week"] == "mon-fri"
    assert daily_job["hour"] == "8-14"
    assert daily_job["minute"] == "5,35"
    assert daily_job["replace_existing"] is True

    watchlist_job = next(job for job in fake_scheduler.jobs if job["id"] == "watchlist_cache")
    assert watchlist_job["trigger"] == "interval"
    assert watchlist_job["minutes"] == 5
    assert watchlist_job["replace_existing"] is True
