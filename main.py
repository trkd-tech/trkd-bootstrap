import os
import logging
from datetime import date, datetime, timedelta
from collections import defaultdict

from flask import Flask
import gspread
from google.auth import default
from kiteconnect import KiteConnect, KiteTicker

# ================== GLOBAL STATE ==================

tick_engine_started = False

# Track A: realtime 1-minute candles
# {(instrument_token, minute_start): candle_dict}
candles_1m = {}

# Track last seen minute per instrument
last_minute_seen = {}

# ================== LOGGING ==================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================== FLASK ==================

app = Flask(__name__)

@app.route("/")
def health_check():
    return "TRKD bootstrap service running", 200

# ================== BOOTSTRAP ==================

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

# ================== INSTRUMENT RESOLUTION ==================

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

# ================== TICK → 1M CANDLES ==================

def detect_minute_close(token, current_minute):
    last_minute = last_minute_seen.get(token)

    if last_minute is None:
        last_minute_seen[token] = current_minute
        return

    if current_minute > last_minute:
        closed_key = (token, last_minute)
        closed_candle = candles_1m.get(closed_key)

        if closed_candle:
            log_closed_1m_candle(token, closed_candle)

        last_minute_seen[token] = current_minute

def process_tick_to_1m(tick):
    if "exchange_timestamp" not in tick:
        return

    if tick.get("last_price") is None:
        return

    token = tick["instrument_token"]
    price = tick["last_price"]
    volume = tick.get("volume_traded", 0)

    ts = tick["exchange_timestamp"]
    minute = ts.replace(second=0, microsecond=0)

    key = (token, minute)
    candle = candles_1m.get(key)

    if candle is None:
        candles_1m[key] = {
            "start": minute,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume
        }
    else:
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price
        candle["volume"] = volume

    detect_minute_close(token, minute)

def log_closed_1m_candle(token, candle):
    logger.info(
        f"1M CLOSED | token={token} | "
        f"{candle['start']} | "
        f"O={candle['open']} "
        f"H={candle['high']} "
        f"L={candle['low']} "
        f"C={candle['close']} "
        f"V={candle['volume']}"
    )

# ================== WEBSOCKET ==================

def start_kite_ticker(tokens):
    kws = KiteTicker(
        api_key=os.getenv("KITE_API_KEY"),
        access_token=os.getenv("KITE_ACCESS_TOKEN")
    )

    def on_connect(ws, response):
        global tick_engine_started
        tick_engine_started = True
        logger.info("Tick engine started")
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_QUOTE, tokens)

    def on_ticks(ws, ticks):
        for tick in ticks:
            process_tick_to_1m(tick)

    def on_close(ws, code, reason):
        logger.warning(f"Kite WebSocket closed: {code} {reason}")

    kws.on_connect = on_connect
    kws.on_ticks = on_ticks
    kws.on_close = on_close

    kws.connect(threaded=True)

# ================== KITE REST ==================

def kite_rest_check():
    logger.info("=== KITE REST CHECK START ===")

    kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
    kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN"))

    profile = kite.profile()
    logger.info(f"Kite user: {profile.get('user_name')}")

    nifty_token = resolve_current_month_fut(kite, "NIFTY")
    banknifty_token = resolve_current_month_fut(kite, "BANKNIFTY")

    start_kite_ticker([nifty_token, banknifty_token])

    logger.info("Track A ready: waiting for live ticks")
    logger.info("=== KITE REST CHECK SUCCESS ===")

# ================== ENTRYPOINT ==================

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
