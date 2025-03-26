# app.py

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
from routes.financials_routes import financials_blueprint  
from routes.user_routes import user_blueprint

# Import your scheduled job wrapper
from tasks.daily_scan_tasks import daily_scan_wrapper

def create_app(testing=False):
    """
    Application factory that configures and returns the Flask app.
    """
    load_dotenv()
    if testing:
        frontend_origin = "*"
    else:
        frontend_origin = os.getenv('front_end_client_website')
    
    app = Flask(__name__)
    app.config["TESTING"] = testing

    # Set up CORS
    CORS(app, resources={r"/api/*": {"origins": frontend_origin}})
    
    # Register the blueprint modules
    app.register_blueprint(summary_blueprint)
    app.register_blueprint(alerts_blueprint)
    app.register_blueprint(tickers_blueprint)
    app.register_blueprint(option_price_ratio_blueprint)
    app.register_blueprint(financials_blueprint)
    app.register_blueprint(user_blueprint)

    return app

def create_scheduler():
    """
    Create and start the BackgroundScheduler for running daily_scan.
    """
    scheduler = BackgroundScheduler()
    scheduler.add_job(daily_scan_wrapper, 'cron', day_of_week='mon-fri', hour=16, minute=30)
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))
    return scheduler

if __name__ == "__main__":
    app = create_app()
    daily_scan_wrapper()  # Optionally run once on startup in dev mode
    create_scheduler()
    app.run(debug=True)
