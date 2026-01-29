import os
import logging
from flask import Flask
import gspread
from google.auth import default
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker


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


def kite_rest_check():
    logger.info("=== KITE REST CHECK START ===")

    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")

    if not api_key or not access_token:
        raise Exception("Kite credentials missing")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    profile = kite.profile()
    logger.info(f"Kite user: {profile.get('user_name')}")

    instruments = kite.instruments("NFO")
    logger.info(f"NFO instruments loaded: {len(instruments)}")

    tokens = resolve_futures_tokens(kite)
    start_kite_ticker(kite, tokens)

    logger.info("=== KITE REST CHECK SUCCESS ===")

def resolve_futures_tokens(kite):
    instruments = kite.instruments("NFO")

    nifty_fut = None
    banknifty_fut = None

    for ins in instruments:
        if ins["tradingsymbol"].startswith("NIFTY") and ins["instrument_type"] == "FUT":
            nifty_fut = ins
        if ins["tradingsymbol"].startswith("BANKNIFTY") and ins["instrument_type"] == "FUT":
            banknifty_fut = ins

    if not nifty_fut or not banknifty_fut:
        raise Exception("Could not resolve FUT instruments")

    logger.info(f"NIFTY FUT: {nifty_fut['tradingsymbol']} ({nifty_fut['instrument_token']})")
    logger.info(f"BANKNIFTY FUT: {banknifty_fut['tradingsymbol']} ({banknifty_fut['instrument_token']})")

    return [
        nifty_fut["instrument_token"],
        banknifty_fut["instrument_token"]
    ]

def start_kite_ticker(kite, tokens):
    kws = KiteTicker(
        api_key=os.getenv("KITE_API_KEY"),
        access_token=os.getenv("KITE_ACCESS_TOKEN")
    )

    def on_connect(ws, response):
        logger.info("Kite WebSocket connected")
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_LTP, tokens)

    def on_ticks(ws, ticks):
        for tick in ticks:
            logger.info(f"TICK {tick['instrument_token']} LTP={tick['last_price']}")

    def on_close(ws, code, reason):
        logger.warning(f"Kite WebSocket closed: {code} {reason}")

    kws.on_connect = on_connect
    kws.on_ticks = on_ticks
    kws.on_close = on_close

    kws.connect(threaded=True)


def safe_bootstrap():
    try:
        bootstrap_checks()
        kite_rest_check()
        logger.info("=== BOOTSTRAP + KITE REST CHECK COMPLETED ===")
    except Exception:
        logger.exception("BOOTSTRAP FAILED (non-fatal)")

safe_bootstrap()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

