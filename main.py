"""
TRKD — Algorithmic Trading Runtime (Monolith v1)

This file intentionally contains all logic end-to-end for:
- Market data ingestion (Kite WebSocket + REST)
- Candle aggregation (1m, 5m)
- Indicator computation (VWAP, Opening Range)
- Strategy signal generation (VWAP ORB)
- Paper execution & exits (risk engine)

Once Strategy 1 is validated in live markets, this file
will be split by responsibility into:

- data/        → ticks, candles
- indicators/  → vwap, opening range
- strategies/  → vwap_orb
- execution/   → paper, live
- risk/        → exits, trailing SL

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

from flask import Flask
import gspread
from google.auth import default
from kiteconnect import KiteConnect, KiteTicker

# ============================================================
# GLOBAL CONFIG & STATE
# ============================================================

EXECUTION_MODE = "PAPER"        # "LIVE" later
LIVE_TRADING_ENABLED = False   # HARD SAFETY SWITCH

# token -> {"index": "NIFTY" / "BANKNIFTY"}
token_meta = {}

# ------------------------------------------------------------
# DATA ENGINE STATE (future: data/)
# ------------------------------------------------------------

candles_1m = {}       # (token, minute_start) -> candle
candles_5m = {}       # (token, five_min_start) -> candle
last_minute_seen = {} # token -> last minute timestamp

# ------------------------------------------------------------
# INDICATORS STATE (future: indicators/)
# ------------------------------------------------------------

vwap_state = {}       # token -> {cum_pv, cum_vol, vwap}
opening_range = {}   # token -> {high, low, finalized}

# ------------------------------------------------------------
# STRATEGY STATE (future: strategies/)
# ------------------------------------------------------------

strategy_state = {}  # token -> {signal, triggered, date}

# ------------------------------------------------------------
# EXECUTION STATE (future: execution/)
# ------------------------------------------------------------

positions = {}       # token -> position dict

PAPER_QTY = {
    "NIFTY": 50,
    "BANKNIFTY": 15
}

# ------------------------------------------------------------
# RISK CONFIG (future: risk/)
# ------------------------------------------------------------

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
# FLASK (health check only — Cloud Run requirement)
# ============================================================

app = Flask(__name__)

@app.route("/")
def health_check():
    return "TRKD runtime alive", 200

# ============================================================
# BOOTSTRAP (infra + config validation)
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
# DATA ENGINE — TICKS → 1-MIN CANDLES
# ============================================================

def process_tick_to_1m(tick):
    if "exchange_timestamp" not in tick or tick.get("last_price") is None:
        return

    token = tick["instrument_token"]
    ts = tick["exchange_timestamp"].replace(second=0, microsecond=0)
    price = tick["last_price"]

    key = (token, ts)
    candle = candles_1m.get(key)

    if candle is None:
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

def detect_minute_close(token, minute):
    last = last_minute_seen.get(token)
    if last and minute > last:
        log_closed_1m(token, last)
        aggregate_5m_from_1m(token, last)
    last_minute_seen[token] = minute

def log_closed_1m(token, minute):
    candle = candles_1m.get((token, minute))
    if not candle:
        return
    logger.info(
        f"1M CLOSED | token={token} | {minute} | "
        f"O={candle['open']} H={candle['high']} "
        f"L={candle['low']} C={candle['close']} V={candle['volume']}"
    )

# ============================================================
# 5-MIN CANDLES
# ============================================================

def aggregate_5m_from_1m(token, closed_minute):
    five_start = closed_minute.replace(
        minute=(closed_minute.minute // 5) * 5,
        second=0,
        microsecond=0
    )

    key = (token, five_start)
    if key in candles_5m:
        return

    minutes = [five_start + timedelta(minutes=i) for i in range(5)]
    parts = [candles_1m.get((token, m)) for m in minutes]

    if any(p is None for p in parts):
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

    logger.info(
        f"5M CLOSED | token={token} | {five_start} | "
        f"O={candle['open']} H={candle['high']} "
        f"L={candle['low']} C={candle['close']} V={candle['volume']}"
    )

    update_vwap(token, candle)
    update_opening_range(token, candle)
    evaluate_orb_breakout(token, candle)
    check_vwap_recross_exit(token, candle)

# ============================================================
# INDICATORS — VWAP & OPENING RANGE
# ============================================================

def update_vwap(token, candle):
    tp = (candle["high"] + candle["low"] + candle["close"]) / 3
    pv = tp * candle["volume"]

    s = vwap_state.setdefault(token, {"cum_pv": 0, "cum_vol": 0, "vwap": None})
    s["cum_pv"] += pv
    s["cum_vol"] += candle["volume"]
    s["vwap"] = s["cum_pv"] / s["cum_vol"]
    #========Temp logger
    logger.info(
        f"VWAP | token={token} | "
        f"upto={candle['start']} | "
        f"VWAP={round(s['vwap'], 2)}"
    )
    #========Temp logger ends

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
        logger.info(f"OPENING RANGE FINALIZED | token={token} | H={s['high']} L={s['low']}")

# ============================================================
# STRATEGY — VWAP ORB
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

    #=== Temp logger
    logger.info(
        f"ORB CHECK | token={token} | "
        f"close={candle['close']} | "
        f"OR=({orr['low']},{orr['high']}) | "
        f"VWAP={round(vwap,2)}"
    )
    #=== Temp logger Ends

# ============================================================
# EXECUTION — PAPER
# ============================================================

def paper_enter_position(token, signal, candle):
    if token in positions and positions[token]["open"]:
        return

    index = token_meta[token]["index"]
    price = candle["close"]

    positions[token] = {
        "direction": signal,
        "entry_price": price,
        "entry_time": candle["start"],
        "qty": PAPER_QTY[index],
        "open": True,
        "best_price": price
    }

    logger.info(f"PAPER ENTRY | token={token} | {signal} @ {price}")

def paper_exit_position(token, price, reason):
    pos = positions.get(token)
    if not pos or not pos["open"]:
        return

    pnl = (
        (price - pos["entry_price"]) * pos["qty"]
        if pos["direction"] == "LONG"
        else (pos["entry_price"] - price) * pos["qty"]
    )

    pos["open"] = False
    logger.info(f"PAPER EXIT | token={token} | {reason} | PNL={round(pnl,2)}")

# ============================================================
# RISK ENGINE
# ============================================================

def check_trailing_sl(tick):
    token = tick["instrument_token"]
    pos = positions.get(token)
    if not pos or not pos["open"]:
        return

    ltp = tick["last_price"]
    index = token_meta[token]["index"]
    trail = TRAIL_SL_POINTS[index]

    if pos["direction"] == "LONG":
        pos["best_price"] = max(pos["best_price"], ltp)
        if pos["best_price"] - ltp >= trail:
            paper_exit_position(token, ltp, "TRAIL_SL")

def check_vwap_recross_exit(token, candle):
    pos = positions.get(token)
    if not pos or not pos["open"]:
        return

    vwap = vwap_state[token]["vwap"]
    close = candle["close"]

    if pos["direction"] == "LONG" and close < vwap:
        paper_exit_position(token, close, "VWAP_RECROSS")

def check_time_exit(tick):
    if tick["exchange_timestamp"].strftime("%H:%M") >= TIME_EXIT_HHMM:
        paper_exit_position(
            tick["instrument_token"],
            tick["last_price"],
            "TIME_EXIT"
        )

# ============================================================
# HEARTBEAT (debug liveness)
# ============================================================

def heartbeat():
    while True:
        logger.info("SYSTEM ALIVE | waiting for ticks")
        time.sleep(60)

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
            except Exception:
                logger.exception("Tick error (non-fatal)")

    def on_close(ws, code, reason):
        logger.warning(f"Kite WebSocket closed: {code} {reason}")

    kws.on_connect = on_connect
    kws.on_ticks = on_ticks
    kws.on_close = on_close

    kws.connect(threaded=True)

# ============================================================
# BOOTSTRAP THREAD
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
