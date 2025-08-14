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

# Scheduled job wrapper
from tasks.daily_scan_tasks import daily_scan_wrapper

def create_app(testing=False):
    """
    Application factory that configures and returns the Flask app.
    """
    load_dotenv()
    frontend_origin = "*" if testing else os.getenv("front_end_client_website")

    app = Flask(__name__)
    app.config["TESTING"] = testing

    # CORS
    CORS(app, resources={r"/api/*": {"origins": frontend_origin}})

    # Blueprints
    app.register_blueprint(summary_blueprint)
    app.register_blueprint(alerts_blueprint)
    app.register_blueprint(tickers_blueprint)
    app.register_blueprint(option_price_ratio_blueprint)
    app.register_blueprint(financials_blueprint)
    app.register_blueprint(user_blueprint, url_prefix="/api")
    app.register_blueprint(backtest_blueprint)
    app.register_blueprint(ticker_logo_blueprint)

    return app

def create_scheduler():
    """
    Background scheduler pinned to America/Chicago.
    Job runs Mon–Fri at 16:02 local Chicago time.
    """
    chicago = timezone("America/Chicago")
    scheduler = BackgroundScheduler(
        timezone=chicago,
        job_defaults={
            "misfire_grace_time": 300,  # 5 min grace
            "coalesce": True            # if we missed one run, do it once
        },
    )

    scheduler.add_job(
        daily_scan_wrapper,
        trigger="cron",
        id="daily_scan",
        day_of_week="mon-fri",
        hour=16,
        minute=2,
    )

    def _log(event):
        # NOTE: uses the global `app` defined below in __main__
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
    # Optional: do a one-time warm scan only in development
    if app.config.get("DEBUG", True):
        daily_scan_wrapper()
    create_scheduler()
    app.run(debug=True)
