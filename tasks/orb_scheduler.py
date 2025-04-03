# tasks/orb_scheduler.py

import os
import logging
import datetime
import pytz
from dotenv import load_dotenv

# Import the SMS helper and the breakout test function.
from tasks.textbelt_utils import send_textbelt_sms
from analysis.opening_range_breakout import run_opening_range_breakout_tests

# Load environment variables.
load_dotenv()
PHONE_NUMBER = os.getenv("PHONE_NUMBER")
if not PHONE_NUMBER:
    raise ValueError("PHONE_NUMBER not found in environment variables.")

# Hard-coded list of tickers to check.
TICKERS = ["AMZN", "LULU", "META", "QQQ", "TSLA"]

# Global state to accumulate breakouts.
pending_breakouts = {}  # Mapping: ticker -> breakout direction ("long" or "short")
first_breakout_time = None
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
    It scans each ticker for a breakout. When a breakout is detected, it records
    the ticker and the recommended action ("long" or "short") into a global dictionary.
    Once the first breakout is detected, the scanner continues for 5 minutes to collect
    any additional signals before sending a consolidated SMS and canceling the job.
    """
    global sms_sent, first_breakout_time, pending_breakouts

    try:
        now = datetime.datetime.now(pytz.timezone("US/Eastern"))
        logger.info("Scanning for breakouts...")

        for ticker in TICKERS:
            # Skip if a breakout for this ticker is already recorded.
            if ticker in pending_breakouts:
                continue

            try:
                # Use a 1-minute interval (days=1 gets today's data).
                result = run_opening_range_breakout_tests(ticker, days=1, interval="1m")
            except Exception as e:
                logger.error(f"Error scanning ticker {ticker}: {e}")
                continue

            scenarios = result.get("scenarios", [])
            for scenario in scenarios:
                if scenario.get("num_trades", 0) > 0:
                    # Extract breakout direction from the first trade.
                    daily_trades = scenario.get("daily_trades", [])
                    if daily_trades:
                        direction = daily_trades[0].get("direction", "unknown")
                    else:
                        direction = "unknown"
                    pending_breakouts[ticker] = direction
                    logger.info(f"Breakout detected for {ticker}: {direction}")
                    # Record the first breakout time if not already set.
                    if not first_breakout_time:
                        first_breakout_time = now
                        logger.info(f"First breakout detected at {first_breakout_time.isoformat()}")
                    break  # Move to next ticker once breakout is detected.

        # If we've detected any breakout, check if 5 minutes have passed since the first detection.
        if first_breakout_time:
            elapsed_minutes = (now - first_breakout_time).total_seconds() / 60.0
            if elapsed_minutes >= 5:
                if pending_breakouts:
                    # Build a consolidated message including ticker and action.
                    message = "Breakouts detected: " + ", ".join(
                        [f"{ticker} {direction}" for ticker, direction in pending_breakouts.items()]
                    )
                    send_textbelt_sms(PHONE_NUMBER, message)
                    logger.info(f"SMS sent: {message}")
                else:
                    logger.info("No breakouts to notify after accumulation period.")
                sms_sent = True
                # Cancel the scanner job.
                try:
                    scheduler.remove_job("orb_breakout_scanner")
                    logger.info("Breakout scanner job canceled after 5-minute accumulation.")
                except Exception as e:
                    logger.error(f"Error canceling scanner job: {e}")
        else:
            logger.info("No breakouts detected yet.")

    except Exception as e:
        logger.error(f"Unexpected error in breakout scanning: {e}")

def start_breakout_scanner(scheduler):
    """
    Schedules the breakout scanner to run every 2 minutes,
    starting at 10:00 AM Eastern and ending 2 hours later (at 12:00 PM Eastern).
    """
    eastern = pytz.timezone("US/Eastern")
    now = datetime.datetime.now(eastern)

    # Determine the next 10:00 AM Eastern start time.
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
