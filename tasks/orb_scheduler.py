# tasks/orb_scheduler.py

import os
import logging
import datetime
import pytz
from dotenv import load_dotenv

# Import the SMS helper and your breakout test function
from tasks.textbelt_utils import send_textbelt_sms
from analysis.opening_range_breakout import run_opening_range_breakout_tests

# Load environment variables
load_dotenv()
PHONE_NUMBER = os.getenv("PHONE_NUMBER")
if not PHONE_NUMBER:
    raise ValueError("PHONE_NUMBER not found in environment variables.")

# Hard-coded list of tickers to check
TICKERS = ["AMZN", "lulu", "meta", "QQQ", "TSLA"]

sms_sent = False

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def check_opening_range_breakouts(scheduler):
    """
    This function is called every 2 minutes between 10:00 AM and 12:00 PM Eastern.
    It checks each ticker for a breakout and, if one is found, sends an SMS notification
    and cancels the scheduled job.
    """
    global sms_sent

    try:
        breakouts_found = []
        logger.info("Scanning for breakouts...")

        for ticker in TICKERS:
            try:
                # Use a 1-minute interval to get near-live data (days=1 gets today's data)
                result = run_opening_range_breakout_tests(ticker, days=1, interval="1m")
            except Exception as e:
                logger.error(f"Error scanning ticker {ticker}: {e}")
                continue

            scenarios = result.get("scenarios", [])
            for scenario in scenarios:
                if scenario.get("num_trades", 0) > 0:
                    logger.info(f"Breakout detected for {ticker} with scenario: {scenario['scenario_name']}")
                    breakouts_found.append(ticker)
                    break  # Once a breakout is detected for this ticker, move on.

        if breakouts_found:
            message = f"Breakout detected for: {', '.join(breakouts_found)}"
            send_textbelt_sms(PHONE_NUMBER, message)
            logger.info(f"SMS sent: {message}")
            sms_sent = True

            # Cancel the breakout scanner job since a breakout was found.
            try:
                scheduler.remove_job("orb_breakout_scanner")
                logger.info("Breakout scanner job canceled after sending SMS.")
            except Exception as e:
                logger.error(f"Error canceling scanner job: {e}")
        else:
            logger.info("No breakout detected in this scan.")

    except Exception as e:
        logger.error(f"Unexpected error in breakout scanning: {e}")

def start_breakout_scanner(scheduler):
    """
    Schedules the breakout scanner to run every 2 minutes,
    starting at 10:00 AM Eastern and ending 2 hours later (at 12:00 PM Eastern).
    """
    eastern = pytz.timezone("US/Eastern")
    now = datetime.datetime.now(eastern)

    # Determine the next 10:00 AM Eastern start time
    start_time = now.replace(hour=10, minute=0, second=0, microsecond=0)
    if start_time < now:
        start_time += datetime.timedelta(days=1)
    end_time = start_time + datetime.timedelta(hours=2)

    from apscheduler.triggers.interval import IntervalTrigger
    trigger = IntervalTrigger(
        minutes=2,
        start_date=start_time,
        end_date=end_time,
        timezone=eastern,
    )

    scheduler.add_job(
        func=lambda: check_opening_range_breakouts(scheduler),
        trigger=trigger,
        id="orb_breakout_scanner",
        max_instances=1,
        coalesce=True,
    )
    logger.info(f"Breakout scanner scheduled from {start_time} to {end_time} Eastern, every 2 minutes.")
