# app.py
import os
import atexit
import ipaddress
import re
from urllib.parse import urlparse

from flask import Flask, make_response, request
from flask_cors import CORS
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler import events
from pytz import timezone

# Blueprints
from routes.summary_routes import summary_blueprint
from routes.alerts_routes import alerts_blueprint
from routes.tickers_routes import tickers_blueprint
from routes.Option_price_ratio_routes import option_price_ratio_blueprint
from routes.financials_routes import financials_blueprint
from routes.user_routes import user_blueprint
from routes.backtest_routes import backtest_blueprint
from routes.ticker_logo_routes import ticker_logo_blueprint
from routes.world_markets_routes import world_markets_blueprint

# Scheduled job wrapper
from tasks.daily_scan_tasks import daily_scan_wrapper, prime_scan_cache
from tasks.watchlist_cache_tasks import refresh_watchlist_cache
from tasks.entry_decision_preload_tasks import (
    mark_backend_request_finished,
    mark_backend_request_started,
    preload_entry_decisions_for_startup_alerts,
    preload_entry_decisions_from_latest_alerts,
    refresh_entry_decisions_for_latest_alerts_after_close,
)

_REQUEST_ACTIVITY_EXCLUDED_PATHS = {
    "/api/alerts/latest",
    "/api/alerts/stream",
}

_LOCAL_FRONTEND_ORIGINS = (
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:3001",
    "http://127.0.0.1:3001",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
)


def _should_track_request_activity() -> bool:
    if request.method == "OPTIONS":
        return False
    return request.path not in _REQUEST_ACTIVITY_EXCLUDED_PATHS


def _split_cors_origins(value: str | None) -> list[str]:
    if not value:
        return []
    return [origin.strip().rstrip("/") for origin in re.split(r"[\s,]+", value) if origin.strip()]


def _cors_origins(testing: bool = False):
    if testing:
        return "*"

    configured = _split_cors_origins(
        os.getenv("front_end_client_website") or os.getenv("FRONT_END_CLIENT_WEBSITE")
    )
    if "*" in configured:
        return "*"

    origins = [*configured]
    for origin in _LOCAL_FRONTEND_ORIGINS:
        if origin not in origins:
            origins.append(origin)
    return origins


def _is_private_network_host(hostname: str | None) -> bool:
    if not hostname:
        return False
    host = hostname.lower()
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return address.is_loopback or address.is_private or address.is_link_local


def _cors_origin_for_request(origin: str | None, allowed_origins) -> str | None:
    if not origin:
        return None
    normalized = origin.rstrip("/")
    if allowed_origins == "*" or "*" in allowed_origins:
        return normalized
    if normalized in allowed_origins:
        return normalized

    parsed = urlparse(normalized)
    if parsed.scheme in {"http", "https"} and _is_private_network_host(parsed.hostname):
        return normalized
    return None


def _apply_cors_headers(response, allowed_origins):
    if not request.path.startswith("/api/"):
        return response

    allowed_origin = _cors_origin_for_request(request.headers.get("Origin"), allowed_origins)
    if allowed_origin:
        response.headers["Access-Control-Allow-Origin"] = allowed_origin
        response.headers["Vary"] = "Origin"
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Expose-Headers"] = "Retry-After"
    response.headers["Access-Control-Max-Age"] = "600"
    if request.headers.get("Access-Control-Request-Private-Network"):
        response.headers["Access-Control-Allow-Private-Network"] = "true"
    return response


def create_app(testing=False):
    load_dotenv()

    app = Flask(__name__)
    app.config["TESTING"] = testing
    allowed_cors_origins = _cors_origins(testing)

    CORS(
        app,
        resources={r"/api/*": {"origins": allowed_cors_origins}},
        allow_headers=["Content-Type", "Authorization"],
        expose_headers=["Retry-After"],
        methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    )

    app.register_blueprint(summary_blueprint)
    app.register_blueprint(alerts_blueprint)
    app.register_blueprint(tickers_blueprint)
    app.register_blueprint(option_price_ratio_blueprint)
    app.register_blueprint(financials_blueprint)
    app.register_blueprint(user_blueprint, url_prefix="/api")
    app.register_blueprint(backtest_blueprint)
    app.register_blueprint(ticker_logo_blueprint)
    app.register_blueprint(world_markets_blueprint)

    @app.before_request
    def _handle_api_cors_preflight():
        if request.method != "OPTIONS" or not request.path.startswith("/api/"):
            return
        return _apply_cors_headers(make_response(("", 204)), allowed_cors_origins)

    @app.after_request
    def _add_api_cors_headers(response):
        return _apply_cors_headers(response, allowed_cors_origins)

    @app.before_request
    def _mark_request_started():
        if not _should_track_request_activity():
            return
        mark_backend_request_started()

    @app.teardown_request
    def _mark_request_finished(_exception=None):
        if not _should_track_request_activity():
            return
        mark_backend_request_finished()

    return app

def create_scheduler(app: Flask):
    """
    Background scheduler pinned to America/Chicago.
    Market-driven jobs run only during regular NYSE weekday hours.
    """
    chicago = timezone("America/Chicago")
    scheduler = BackgroundScheduler(
        timezone=chicago,
        job_defaults={
            "misfire_grace_time": 600,   # 10 min grace
            "coalesce": True,            # collapse missed runs to one
            "max_instances": 1,          # this job should never overlap
        },
    )

    scheduler.add_job(
        daily_scan_wrapper,
        trigger="cron",
        id="daily_scan",
        day_of_week="mon-fri",
        hour="8-14",                 # 9:30-16:00 ET session in CT
        minute="5,35",               # two slots per hour
        replace_existing=True,          # if it somehow exists, replace it
    )
    scheduler.add_job(
        refresh_watchlist_cache,
        trigger="interval",
        id="watchlist_cache",
        minutes=5,
        replace_existing=True,
    )
    scheduler.add_job(
        preload_entry_decisions_from_latest_alerts,
        trigger="cron",
        id="entry_decision_preload",
        day_of_week="mon-fri",
        hour=os.getenv("ENTRY_DECISION_PRELOAD_CRON_HOURS", "8-14"),
        minute=os.getenv("ENTRY_DECISION_PRELOAD_CRON_MINUTES", "*/2"),
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        refresh_entry_decisions_for_latest_alerts_after_close,
        trigger="cron",
        id="entry_decision_after_close_preload",
        day_of_week="mon-fri",
        hour=os.getenv("ENTRY_DECISION_AFTER_CLOSE_PRELOAD_CRON_HOUR", "15"),
        minute=os.getenv("ENTRY_DECISION_AFTER_CLOSE_PRELOAD_CRON_MINUTE", "35"),
        replace_existing=True,
        max_instances=1,
    )

    def _log(event):
        if event.exception:
            app.logger.error("Job %s failed: %s", event.job_id, event.exception)
        else:
            app.logger.info("Job %s executed OK", event.job_id)

    try:
        prime_scan_cache()
    except Exception as exc:  # pragma: no cover - defensive guard
        app.logger.error("Startup scan prime failed: %s", exc)
    else:
        try:
            preload_entry_decisions_for_startup_alerts()
        except Exception as exc:  # pragma: no cover - defensive guard
            app.logger.error("Startup Entry Decision alert preload failed: %s", exc)

    scheduler.add_listener(_log, events.EVENT_JOB_EXECUTED | events.EVENT_JOB_ERROR)
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))
    return scheduler

if __name__ == "__main__":
    app = create_app()

    # Start the scheduler exactly once (avoid Werkzeug auto-reloader duplication)
    should_start = (os.environ.get("WERKZEUG_RUN_MAIN") == "true") or not app.debug
    if should_start:
        create_scheduler(app)

    # Scan cache is primed once in create_scheduler(); cron jobs own official refreshes.
    #
    # On macOS, "localhost" often resolves to IPv6 (::1) while 127.0.0.1 is IPv4.
    # The default Flask dev server binds to 127.0.0.1 only, which makes
    # http://localhost:5000 fail. For local dev, we explicitly bind *both* loopbacks.
    from werkzeug.serving import make_server
    import threading

    port = int(os.environ.get("PORT", "5000"))

    servers = []
    try:
        servers.append(make_server("127.0.0.1", port, app, threaded=True))
    except OSError as exc:
        raise SystemExit(f"Failed to bind IPv4 127.0.0.1:{port}: {exc}")

    try:
        servers.append(make_server("::1", port, app, threaded=True))
    except OSError as exc:
        # If IPv6 binding fails (e.g., disabled), keep IPv4 so 127.0.0.1 still works.
        print(f" ! Warning: failed to bind IPv6 ::1:{port} (localhost may not work): {exc}")
    threads = []

    def _serve(server):
        server.serve_forever()

    for srv in servers:
        t = threading.Thread(target=_serve, args=(srv,), daemon=True)
        threads.append(t)
        t.start()

    if len(servers) > 1:
        print(f" * Running on http://127.0.0.1:{port} and http://localhost:{port} (IPv6 ::1)")
    else:
        print(f" * Running on http://127.0.0.1:{port} (IPv4 only)")

    try:
        # Block main thread; servers run in daemon threads.
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        for srv in servers:
            srv.shutdown()
