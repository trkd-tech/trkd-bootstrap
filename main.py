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

# Track A: realtime 5-minute candles
# {(instrument_token, five_min_start): candle_dict}
candles_5m = {}

# Track A: VWAP state per instrument
vwap_state = {
    # token: {"cum_pv": float, "cum_vol": int, "vwap": float}
}

# Track last seen minute per instrument
last_minute_seen = {}

# Track A: Opening Range per instrument
opening_range = {
    # token: {
    #   "high": float,
    #   "low": float,
    #   "finalized": bool
    # }
}

# ===== STRATEGY STATE =====
strategy_state = {
    # token: {
    #   "signal": None,
    #   "triggered": False
    # }
}


# ===== PAPER TRADING STATE =====

positions = {
    # token: {
    #   "direction": "LONG" / "SHORT",
    #   "entry_price": float,
    #   "entry_time": datetime,
    #   "qty": int,
    #   "open": bool
    # }
}

PAPER_QTY = {
    "NIFTY": 50,
    "BANKNIFTY": 15
}


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
            aggregate_5m_from_1m(token, last_minute)
      
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

# ================== 5 Min logger ==================
def log_closed_5m_candle(token, candle):
    logger.info(
        f"5M CLOSED | token={token} | "
        f"{candle['start']} | "
        f"O={candle['open']} "
        f"H={candle['high']} "
        f"L={candle['low']} "
        f"C={candle['close']} "
        f"V={candle['volume']}"
    )


# ================== 5 Min aggregation logic ==================
def aggregate_5m_from_1m(token, closed_minute):
    """
    Build a 5-minute candle once all 5 underlying 1-minute candles exist.
    """
    five_min_start = closed_minute.replace(
        minute=(closed_minute.minute // 5) * 5,
        second=0,
        microsecond=0
    )

    key_5m = (token, five_min_start)

    if key_5m in candles_5m:
        return  # already built

    minutes = [
        five_min_start + timedelta(minutes=i)
        for i in range(5)
    ]

    one_min_candles = []
    for m in minutes:
        c = candles_1m.get((token, m))
        if not c:
            return  # wait until all 5 exist
        one_min_candles.append(c)

    candles_5m[key_5m] = {
        "start": five_min_start,
        "open": one_min_candles[0]["open"],
        "high": max(c["high"] for c in one_min_candles),
        "low": min(c["low"] for c in one_min_candles),
        "close": one_min_candles[-1]["close"],
        "volume": sum(c["volume"] for c in one_min_candles),
    }

    log_closed_5m_candle(token, candles_5m[key_5m])
    update_vwap(token, candles_5m[key_5m])
    update_opening_range(token, candles_5m[key_5m])
    evaluate_orb_breakout(token, candles_5m[key_5m])


# ================== Open Range Logger ==================
def log_opening_range(token, state):
    logger.info(
        f"OPENING RANGE FINALIZED | token={token} | "
        f"HIGH={state['high']} LOW={state['low']}"
    )


# ================== VWAP update function ==================
def update_vwap(token, candle_5m):
    """
    Update session VWAP using a closed 5-minute candle.
    """
    typical_price = (
        candle_5m["high"] +
        candle_5m["low"] +
        candle_5m["close"]
    ) / 3

    pv = typical_price * candle_5m["volume"]

    state = vwap_state.get(token)

    if state is None:
        state = {
            "cum_pv": 0.0,
            "cum_vol": 0,
            "vwap": None
        }
        vwap_state[token] = state

    state["cum_pv"] += pv
    state["cum_vol"] += candle_5m["volume"]

    if state["cum_vol"] > 0:
        state["vwap"] = state["cum_pv"] / state["cum_vol"]

    log_vwap(token, state["vwap"], candle_5m["start"])

# ================== VWAP Logger ==================
def log_vwap(token, vwap, candle_start):
    logger.info(
        f"VWAP | token={token} | "
        f"upto={candle_start} | "
        f"VWAP={round(vwap, 2)}"
    )

# ================== Opening Range update function ==================
def update_opening_range(token, candle_5m):
    """
    Update opening range between 09:15 and 09:45.
    """
    start = candle_5m["start"].time()

    # Only consider candles from 09:15 to before 09:45
    if not (start >= datetime.strptime("09:15", "%H:%M").time()
            and start < datetime.strptime("09:45", "%H:%M").time()):
        return

    state = opening_range.get(token)

    if state is None:
        state = {
            "high": candle_5m["high"],
            "low": candle_5m["low"],
            "finalized": False
        }
        opening_range[token] = state
    else:
        if state["finalized"]:
            return
        state["high"] = max(state["high"], candle_5m["high"])
        state["low"] = min(state["low"], candle_5m["low"])

    # Finalize at 09:40 candle close (which completes at 09:45)
    if start == datetime.strptime("09:40", "%H:%M").time():
        state["finalized"] = True
        log_opening_range(token, state)

# ================== Opening Range Backfill ==================
def backfill_opening_range(kite, token):
    """
    Backfill Opening Range using historical 5-minute candles.
    Runs only if OR is not already finalized.
    """
    if token in opening_range and opening_range[token]["finalized"]:
        return  # already done

    today = datetime.now().date()

    from_dt = datetime.combine(today, datetime.strptime("09:15", "%H:%M").time())
    to_dt = datetime.combine(today, datetime.strptime("09:45", "%H:%M").time())

    candles = kite.historical_data(
        instrument_token=token,
        from_date=from_dt,
        to_date=to_dt,
        interval="5minute"
    )

    if not candles:
        logger.warning("No historical candles for OR backfill")
        return

    high = max(c["high"] for c in candles)
    low = min(c["low"] for c in candles)

    opening_range[token] = {
        "high": high,
        "low": low,
        "finalized": True
    }

    logger.info(
        f"OPENING RANGE BACKFILLED | token={token} | HIGH={high} LOW={low}"
    )


# ================== Opening Range Breakout Evaluation function  ==================
def evaluate_orb_breakout(token, candle_5m):
    """
    VWAP + Opening Range breakout logic.
    Signal fires at most once per instrument per day.
    """
    or_state = opening_range.get(token)
    vwap_info = vwap_state.get(token)

    if not or_state or not or_state.get("finalized"):
        return

    if not vwap_info or not vwap_info.get("vwap"):
        return

    state = strategy_state.get(token)
    if state and state.get("triggered"):
        return  # already triggered today

    close = candle_5m["close"]
    vwap = vwap_info["vwap"]

    signal = None

    if close > or_state["high"] and close > vwap:
        signal = "LONG"

    elif close < or_state["low"] and close < vwap:
        signal = "SHORT"

    if signal:
        strategy_state[token] = {
            "signal": signal,
            "triggered": True
        }

        log_signal(token, signal, candle_5m)
        paper_enter_position(token, signal, candle_5m)

# ================== Signal Logger ==================
def log_signal(token, signal, candle):
    logger.info(
        f"SIGNAL GENERATED | token={token} | "
        f"TYPE={signal} | "
        f"CANDLE={candle['start']} | "
        f"CLOSE={candle['close']}"
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
        ws.set_mode(ws.MODE_FULL, tokens)

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

    # Backfill OR if needed
    backfill_opening_range(kite, nifty_token)
    backfill_opening_range(kite, banknifty_token)
    
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
