import os
import atexit

from flask import Flask
from flask_cors import CORS
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler

# Import your blueprints
from routes.summary_routes import summary_blueprint
from routes.alerts_routes import alerts_blueprint
from routes.tickers_routes import tickers_blueprint
from routes.Option_price_ratio_routes import option_price_ratio_blueprint

# Import your scheduled job wrapper
from tasks.daily_scan_tasks import daily_scan_wrapper

def create_app():
    """
    Application factory that configures and returns the Flask app.
    """
    load_dotenv()
    frontend_origin = os.getenv('front_end_client_website')

    app = Flask(__name__)
    CORS(app, resources={r"/api/*": {"origins": frontend_origin}})

    # Register the blueprint modules
    app.register_blueprint(summary_blueprint)
    app.register_blueprint(alerts_blueprint)
    app.register_blueprint(tickers_blueprint)
    app.register_blueprint(option_price_ratio_blueprint)

    return app

def create_scheduler():
    """
    Create and start the BackgroundScheduler for running daily_scan.
    """
    scheduler = BackgroundScheduler()
    # 4:30 PM on weekdays
    scheduler.add_job(daily_scan_wrapper, 'cron', day_of_week='mon-fri', hour=16, minute=30)
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))
    return scheduler

# Only run the server if this file is executed directly (development mode).
if __name__ == "__main__":
    app = create_app()

    # Optionally run the job once on startup for dev
    daily_scan_wrapper()

    # Start the scheduler
    create_scheduler()

    # Run the app
    app.run(debug=True)
