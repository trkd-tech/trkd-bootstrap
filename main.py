import os
import logging
from datetime import date

from flask import Flask
import gspread
from google.auth import default
from kiteconnect import KiteConnect, KiteTicker

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
        logger.info(f"Secret {key} present: {os.getenv(key) is not None}")

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


ALLOWED_FUT_NAMES = {
    "NIFTY": "NIFTY",
    "BANKNIFTY": "BANKNIFTY"
}


def resolve_current_month_fut(kite, index_name):
    instruments = kite.instruments("NFO")
    target_name = ALLOWED_FUT_NAMES[index_name]

    candidates = [
        ins for ins in instruments
        if ins["segment"] == "NFO-FUT"
        and ins["instrument_type"] == "FUT"
        and ins["name"] == target_name
        and ins["tradingsymbol"].startswith(target_name)
        and ins["expiry"] >= date.today()
    ]

    if not candidates:
        raise Exception(f"No valid FUT found for {index_name}")

    candidates.sort(key=lambda x: x["expiry"])
    selected = candidates[0]

    logger.info(
        f"SELECTED FUT → {index_name} | "
        f"{selected['tradingsymbol']} | "
        f"Expiry={selected['expiry']} | "
        f"Token={selected['instrument_token']}"
    )

    return selected["instrument_token"]


def start_kite_ticker(tokens):
    kws = KiteTicker(
        api_key=os.getenv("KITE_API_KEY"),
        access_token=os.getenv("KITE_ACCESS_TOKEN")
    )

    def on_connect(ws, response):
        logger.info("Kite WebSocket connected")
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_QUOTE, tokens)

    def on_ticks(ws, ticks):
        for tick in ticks:
            logger.info(
                f"TICK {tick['instrument_token']} "
                f"LTP={tick.get('last_price')} "
                f"VOL={tick.get('volume_traded', 0)} "
                f"OI={tick.get('oi', 0)}"
            )

    def on_close(ws, code, reason):
        logger.warning(f"Kite WebSocket closed: {code} {reason}")

    kws.on_connect = on_connect
    kws.on_ticks = on_ticks
    kws.on_close = on_close

    kws.connect(threaded=True)


def kite_rest_check():
    logger.info("=== KITE REST CHECK START ===")

    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    profile = kite.profile()
    logger.info(f"Kite user: {profile.get('user_name')}")

    logger.info(f"NFO instruments loaded: {len(kite.instruments('NFO'))}")

    nifty_token = resolve_current_month_fut(kite, "NIFTY")
    banknifty_token = resolve_current_month_fut(kite, "BANKNIFTY")

    start_kite_ticker([nifty_token, banknifty_token])

    logger.info("=== KITE REST CHECK SUCCESS ===")


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
