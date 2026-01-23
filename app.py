# app.py
import os
import atexit
from flask import Flask
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
from tasks.daily_scan_tasks import daily_scan_wrapper
from tasks.watchlist_cache_tasks import refresh_watchlist_cache

def create_app(testing=False):
    load_dotenv()
    frontend_origin = "*" if testing else os.getenv("front_end_client_website")

    app = Flask(__name__)
    app.config["TESTING"] = testing

    CORS(
        app,
        resources={r"/api/*": {"origins": frontend_origin}},
        allow_headers=["Content-Type", "Authorization"],
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

    return app

def create_scheduler(app: Flask):
    """
    Background scheduler pinned to America/Chicago.
    Job runs Mon–Fri at 16:02 local Chicago time.
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
        hour=16,
        minute=2,
        replace_existing=True,          # if it somehow exists, replace it
    )
    scheduler.add_job(
        refresh_watchlist_cache,
        trigger="interval",
        id="watchlist_cache",
        minutes=5,
        replace_existing=True,
    )

    def _log(event):
        if event.exception:
            app.logger.error("Job %s failed: %s", event.job_id, event.exception)
        else:
            app.logger.info("Job %s executed OK", event.job_id)

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

    # IMPORTANT: no warm scan on startup; the 4:02 PM job owns “official” runs
    app.run(debug=False)
