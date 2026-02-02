"""
TRKD — Algorithmic Trading Runtime (Monolith v1)

End-to-end runtime for:
- Market data ingestion (Kite WebSocket + REST)
- Candle aggregation (1m, 5m)
- Indicator computation (VWAP, Opening Range)
- Strategy signal generation (VWAP ORB)
- Paper execution & exits

DO NOT prematurely refactor.
Stability > purity.
"""

# ============================================================
# IMPORTS
# ============================================================

import os
import logging
import threading
import time
from datetime import date, datetime, timedelta
import pytz

from flask import Flask
import gspread
from google.auth import default
from kiteconnect import KiteConnect, KiteTicker

# ============================================================
# GLOBAL CONFIG & STATE
# ============================================================

EXECUTION_MODE = "PAPER"
LIVE_TRADING_ENABLED = False

tick_engine_started = False

# token -> {"index": "NIFTY" / "BANKNIFTY"}
token_meta = {}

IST = pytz.timezone("Asia/Kolkata")

# ============================================================
# DATA / INDICATOR / STRATEGY STATE
# ============================================================

candles_1m = {}
candles_5m = {}
last_minute_seen = {}

vwap_state = {}
opening_range = {}
strategy_state = {}

positions = {}

PAPER_QTY = {
    "NIFTY": 50,
    "BANKNIFTY": 15
}

TRAIL_SL_POINTS = {
    "NIFTY": 40,
    "BANKNIFTY": 120
}

TIME_EXIT_HHMM = "15:20"

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# FLASK (health only)
# ============================================================

app = Flask(__name__)

@app.route("/")
def health_check():
    return "TRKD runtime alive", 200


# ============================================================
# Ticket from UTC to IST
# ============================================================

def to_ist(ts):
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=pytz.utc)
    return ts.astimezone(IST)

# ============================================================
# HEARTBEAT (OBSERVABILITY)
# ============================================================

def heartbeat():
    while True:
        logger.info("SYSTEM ALIVE | waiting for ticks")
        time.sleep(60)

# ============================================================
# BOOTSTRAP
# ============================================================

def bootstrap_checks():
    logger.info("=== BOOTSTRAP START ===")

    sheet_id = os.getenv("GOOGLE_SHEET_ID", "NOT_SET")
    logger.info(f"GOOGLE_SHEET_ID set: {sheet_id != 'NOT_SET'}")

    for key in ["KITE_API_KEY", "KITE_API_SECRET", "KITE_ACCESS_TOKEN"]:
        logger.info(f"Secret {key} present: {os.getenv(key) is not None}")

    creds, _ = default()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)

    logger.info(f"SYSTEM_CONTROL rows: {len(sh.worksheet('SYSTEM_CONTROL').get_all_records())}")
    logger.info(f"STRATEGIES rows: {len(sh.worksheet('STRATEGIES').get_all_records())}")

    logger.info("=== BOOTSTRAP SUCCESS ===")

# ============================================================
# INSTRUMENT RESOLUTION
# ============================================================

def resolve_current_month_fut(kite, index_name):
    instruments = kite.instruments("NFO")

    candidates = [
        ins for ins in instruments
        if ins["segment"] == "NFO-FUT"
        and ins["instrument_type"] == "FUT"
        and ins["name"] == index_name
        and ins["expiry"] >= date.today()
    ]

    candidates.sort(key=lambda x: x["expiry"])
    selected = candidates[0]

    logger.info(
        f"SELECTED FUT → {index_name} | "
        f"{selected['tradingsymbol']} | "
        f"Expiry={selected['expiry']} | "
        f"Token={selected['instrument_token']}"
    )

    return selected["instrument_token"]

# ============================================================
# TICK → 1M CANDLES
# ============================================================

def process_tick_to_1m(tick):
    if "exchange_timestamp" not in tick or tick.get("last_price") is None:
        return

    token = tick["instrument_token"]
    ts = to_ist(tick["exchange_timestamp"]).replace(second=0, microsecond=0)
    price = tick["last_price"]

    key = (token, ts)
    candle = candles_1m.get(key)

    if not candle:
        candles_1m[key] = {
            "start": ts,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": tick.get("volume_traded", 0)
        }
    else:
        candle["high"] = max(candle["high"], price)
        candle["low"] = min(candle["low"], price)
        candle["close"] = price

    detect_minute_close(token, ts)

def detect_minute_close(token, current_minute):
    """
    Detect transition to a new minute.
    When minute advances, the previous minute is considered CLOSED.
    """
    last_minute = last_minute_seen.get(token)

    if last_minute is None:
        last_minute_seen[token] = current_minute
        return

    if current_minute > last_minute:
        # last_minute is now CLOSED
        closed_key = (token, last_minute)
        candle_1m = candles_1m.get(closed_key)

        if candle_1m:
            logger.info(
                f"1M CLOSED | token={token} | "
                f"{candle_1m['start']} | "
                f"O={candle_1m['open']} "
                f"H={candle_1m['high']} "
                f"L={candle_1m['low']} "
                f"C={candle_1m['close']} "
                f"V={candle_1m['volume']}"
            )

            # Attempt 5-minute aggregation ONLY on minute close
            aggregate_5m_from_1m(token, last_minute)

        last_minute_seen[token] = current_minute


# ============================================================
# 5M CANDLES
# ============================================================

def aggregate_5m_from_1m(token, closed_minute):
    five_start = closed_minute.replace(minute=(closed_minute.minute // 5) * 5)
    key = (token, five_start)

    if key in candles_5m:
        return

    mins = [five_start + timedelta(minutes=i) for i in range(5)]
    parts = [candles_1m.get((token, m)) for m in mins]

   if any(p is None for p in parts):
       logger.info(
            f"5M WAIT | token={token} | bucket={five_start}"
        )
        return


    candle = {
        "start": five_start,
        "open": parts[0]["open"],
        "high": max(p["high"] for p in parts),
        "low": min(p["low"] for p in parts),
        "close": parts[-1]["close"],
        "volume": sum(p["volume"] for p in parts)
    }

    candles_5m[key] = candle

    update_vwap(token, candle)
    update_opening_range(token, candle)
    evaluate_orb_breakout(token, candle)
    check_vwap_recross_exit(token, candle)
#======== Temp Logger
    logger.info(
        f"5M CHECK | token={token} | "
        f"bucket={five_start} | "
        f"minutes={[m.strftime('%H:%M') for m in mins]}"
    )
#======== Temp Logger Ends

# ============================================================
# INDICATORS
# ============================================================

def update_vwap(token, candle):
    tp = (candle["high"] + candle["low"] + candle["close"]) / 3
    pv = tp * candle["volume"]

    s = vwap_state.setdefault(token, {"cum_pv": 0, "cum_vol": 0, "vwap": None})
    s["cum_pv"] += pv
    s["cum_vol"] += candle["volume"]
    s["vwap"] = s["cum_pv"] / s["cum_vol"]

def update_opening_range(token, candle):
    t = candle["start"].time()
    if not (datetime.strptime("09:15","%H:%M").time() <= t < datetime.strptime("09:45","%H:%M").time()):
        return

    s = opening_range.setdefault(token, {
        "high": candle["high"],
        "low": candle["low"],
        "finalized": False
    })

    if s["finalized"]:
        return

    s["high"] = max(s["high"], candle["high"])
    s["low"] = min(s["low"], candle["low"])

    if t == datetime.strptime("09:40","%H:%M").time():
        s["finalized"] = True
        logger.info(f"OPENING RANGE FINALIZED | {token} | H={s['high']} L={s['low']}")

# ============================================================
# STRATEGY
# ============================================================

def evaluate_orb_breakout(token, candle):
    today = candle["start"].date()
    state = strategy_state.get(token)

    if state and state["triggered"] and state["date"] == today:
        return

    orr = opening_range.get(token)
    vw = vwap_state.get(token)

    if not orr or not orr["finalized"] or not vw:
        return

    close = candle["close"]
    vwap = vw["vwap"]

    signal = None
    if close > orr["high"] and close > vwap:
        signal = "LONG"
    elif close < orr["low"] and close < vwap:
        signal = "SHORT"

    if signal:
        strategy_state[token] = {
            "signal": signal,
            "triggered": True,
            "date": today
        }
        paper_enter_position(token, signal, candle)

# ============================================================
# EXECUTION & RISK
# ============================================================

def paper_enter_position(token, signal, candle):
    if token in positions and positions[token]["open"]:
        return

    index = token_meta[token]["index"]
    price = candle["close"]

    positions[token] = {
        "direction": signal,
        "entry_price": price,
        "qty": PAPER_QTY[index],
        "open": True,
        "best_price": price
    }

    logger.info(f"PAPER ENTRY | {token} | {signal} @ {price}")

def paper_exit_position(token, price, reason):
    pos = positions.get(token)
    if not pos or not pos["open"]:
        return

    pnl = (price - pos["entry_price"]) * pos["qty"] \
        if pos["direction"] == "LONG" \
        else (pos["entry_price"] - price) * pos["qty"]

    pos["open"] = False
    logger.info(f"PAPER EXIT | {token} | {reason} | PNL={round(pnl,2)}")

def check_trailing_sl(tick):
    token = tick["instrument_token"]
    pos = positions.get(token)
    if not pos or not pos["open"]:
        return

    ltp = tick["last_price"]
    trail = TRAIL_SL_POINTS[token_meta[token]["index"]]

    if pos["direction"] == "LONG":
        pos["best_price"] = max(pos["best_price"], ltp)
        if pos["best_price"] - ltp >= trail:
            paper_exit_position(token, ltp, "TRAIL_SL")

def check_vwap_recross_exit(token, candle):
    pos = positions.get(token)
    if pos and pos["open"] and candle["close"] < vwap_state[token]["vwap"]:
        paper_exit_position(token, candle["close"], "VWAP_RECROSS")

def check_time_exit(tick):
    if tick["exchange_timestamp"].strftime("%H:%M") >= TIME_EXIT_HHMM:
        paper_exit_position(tick["instrument_token"], tick["last_price"], "TIME_EXIT")

# ============================================================
# WEBSOCKET
# ============================================================

def start_kite_ticker(tokens):
    kws = KiteTicker(
        api_key=os.getenv("KITE_API_KEY"),
        access_token=os.getenv("KITE_ACCESS_TOKEN")
    )

    def on_connect(ws, response):
        logger.info("Kite WebSocket connected")
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)

    def on_ticks(ws, ticks):
        for tick in ticks:
            try:
                process_tick_to_1m(tick)
                check_trailing_sl(tick)
                check_time_exit(tick)
           # ===========TEMPORARY LOGGER =================
                # logger.info(
                #    f"TICK | token={tick['instrument_token']} "
                #    f"LTP={tick.get('last_price')} "
                #    f"VOL={tick.get('volume_traded')}"
                #    )
            # ========== TEMPORARY LOGGER ENDS =============
            except Exception:
                logger.exception("Tick error")

    kws.on_connect = on_connect
    kws.on_ticks = on_ticks
    kws.connect(threaded=True)

# ============================================================
# BACKGROUND ENGINE
# ============================================================

def start_background_engine():
    try:
        bootstrap_checks()
        kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
        kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN"))

        nifty = resolve_current_month_fut(kite, "NIFTY")
        banknifty = resolve_current_month_fut(kite, "BANKNIFTY")

        token_meta[nifty] = {"index": "NIFTY"}
        token_meta[banknifty] = {"index": "BANKNIFTY"}

        start_kite_ticker([nifty, banknifty])

        logger.info("BACKGROUND ENGINE STARTED")
    except Exception:
        logger.exception("Background engine failed")

# ============================================================
# ENTRYPOINT (Cloud Run safe)
# ============================================================

if __name__ == "__main__":
    threading.Thread(target=start_background_engine, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
