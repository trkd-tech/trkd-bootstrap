import os
import logging
from flask import Flask
import gspread
from google.auth import default

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route("/")
def health_check():
    return "TRKD bootstrap service running", 200


def bootstrap_checks():
    logger.info("=== TRKD BOOTSTRAP START ===")

    trading_mode = os.getenv("TRADING_MODE", "UNKNOWN")
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "NOT_SET")
    app_env = os.getenv("APP_ENV", "UNKNOWN")

    logger.info(f"APP_ENV: {app_env}")
    logger.info(f"TRADING_MODE: {trading_mode}")
    logger.info(f"GOOGLE_SHEET_ID set: {sheet_id != 'NOT_SET'}")

    for key in ["KITE_API_KEY", "KITE_API_SECRET", "KITE_ACCESS_TOKEN"]:
        present = os.getenv(key) is not None
        logger.info(f"Secret {key} present: {present}")

    creds, _ = default()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)

    system_control = sh.worksheet("SYSTEM_CONTROL").get_all_records()
    strategies = sh.worksheet("STRATEGIES").get_all_records()

    logger.info(f"SYSTEM_CONTROL rows: {len(system_control)}")
    logger.info(f"STRATEGIES rows: {len(strategies)}")

    if strategies:
        logger.info(f"First strategy loaded: {strategies[0].get('Strategy_ID')}")

    logger.info("=== TRKD BOOTSTRAP SUCCESS ===")


def safe_bootstrap():
    try:
        bootstrap_checks()
    except Exception as e:
        logger.exception("BOOTSTRAP FAILED (non-fatal)")


safe_bootstrap()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
