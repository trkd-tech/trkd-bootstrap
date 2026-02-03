"""
TRKD — Algorithmic Trading Runtime (Monolith v1)

DO NOT prematurely refactor.
Stability > purity.

This file supports:
- Track A: Live ticks → 1m → 5m
- Track B: Historical backfill (VWAP + OR)
- Multiple strategies (ORB + VWAP Crossover)
- Paper execution with exits
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
# GLOBAL CONFIG
# ============================================================

EXECUTION_MODE = "PAPER"
LIVE_TRADING_ENABLED = False

TIME_EXIT_HHMM = "15:20"
OR_START = datetime.strptime("09:15", "%H:%M").time()
OR_END   = datetime.strptime("09:45", "%H:%M").time()

# ============================================================
# GLOBAL STATE
# ============================================================

token_meta = {}

candles_1m = {}
candles_5m = {}
last_minute_seen = {}

vwap_state = {}
opening_range = {}

positions = {}

strategy_state = {}   # token -> strategy_name -> state

# ============================================================
# STRATEGY CONFIG
# ============================================================

STRATEGIES = {
    "VWAP_ORB": {
        "enabled": True,
        "max_trades_per_day": 1
    },
    "VWAP_CROSSOVER": {
        "enabled": True,
        "direction": "BOTH",   # UP / DOWN / BOTH
        "max_trades_per_day": 1,
        "trade_after": "09:45",
        "trade_before": "14:30"
    }
}

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# FLASK (Cloud Run health)
# ============================================================

app = Flask(__name__)

@app.route("/")
def health():
    return "TRKD alive", 200

# ============================================================
# BOOTSTRAP CHECKS
# ============================================================

def bootstrap_checks():
    logger.info("=== BOOTSTRAP START ===")

    for key in ["KITE_API_KEY", "KITE_API_SECRET", "KITE_ACCESS_TOKEN"]:
        logger.info(f"Secret {key} present: {os.getenv(key) is not None}")

    creds, _ = default()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.getenv("GOOGLE_SHEET_ID"))

    logger.info(f"SYSTEM_CONTROL rows: {len(sh.worksheet('SYSTEM_CONTROL').get_all_records())}")
    logger.info(f"STRATEGIES rows: {len(sh.worksheet('STRATEGIES').get_all_records())}")

    logger.info("=== BOOTSTRAP SUCCESS ===")

# ============================================================
# INSTRUMENT RESOLUTION
# ============================================================

def resolve_current_month_fut(kite, index_name):
    instruments = kite.instruments("NFO")
    cands = [
        i for i in instruments
        if i["segment"] == "NFO-FUT"
        and i["instrument_type"] == "FUT"
        and i["name"] == index_name
        and i["expiry"] >= date.today()
    ]
    cands.sort(key=lambda x: x["expiry"])
    sel = cands[0]

    logger.info(
        f"SELECTED FUT → {index_name} | {sel['tradingsymbol']} | "
        f"Expiry={sel['expiry']} | Token={sel['instrument_token']}"
    )
    return sel["instrument_token"]

# ============================================================
# TRACK B — BACKFILL
# ============================================================

def backfill_opening_range(kite, token):
    if token in opening_range and opening_range[token]["finalized"]:
        return

    today = datetime.now().date()
    from_dt = datetime.combine(today, OR_START)
    to_dt   = datetime.combine(today, OR_END)

    candles = kite.historical_data(token, from_dt, to_dt, "5minute")
    if not candles:
        logger.warning(f"OR BACKFILL FAILED | token={token}")
        return

    opening_range[token] = {
        "high": max(c["high"] for c in candles),
        "low":  min(c["low"] for c in candles),
        "finalized": True
    }

    logger.info(
        f"OPENING RANGE BACKFILLED | token={token} | "
        f"H={opening_range[token]['high']} L={opening_range[token]['low']} | candles={len(candles)}"
    )

def backfill_vwap(kite, token):
    now = datetime.now()
    if now.time() < OR_START:
        logger.warning(f"VWAP BACKFILL SKIPPED | token={token} | too early")
        return

    today = now.date()
    candles = kite.historical_data(
        token,
        datetime.combine(today, OR_START),
        now,
        "5minute"
    )

    cum_pv = 0
    cum_vol = 0

    for c in candles:
        tp = (c["high"] + c["low"] + c["close"]) / 3
        cum_pv += tp * c["volume"]
        cum_vol += c["volume"]

    if cum_vol == 0:
        return

    vwap_state[token] = {
        "cum_pv": cum_pv,
        "cum_vol": cum_vol,
        "vwap": cum_pv / cum_vol
    }

    logger.info(
        f"VWAP BACKFILL DONE | token={token} | VWAP={round(vwap_state[token]['vwap'],2)}"
    )

# ============================================================
# TRACK A — TICKS → CANDLES
# ============================================================

def process_tick_to_1m(tick):
    if "exchange_timestamp" not in tick:
        return

    token = tick["instrument_token"]
    ts = tick["exchange_timestamp"].replace(second=0, microsecond=0)
    price = tick["last_price"]

    c = candles_1m.setdefault((token, ts), {
        "start": ts,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": 0
    })

    c["high"] = max(c["high"], price)
    c["low"] = min(c["low"], price)
    c["close"] = price
    c["volume"] = tick.get("volume_traded", c["volume"])

    detect_minute_close(token, ts)

def detect_minute_close(token, minute):
    last = last_minute_seen.get(token)
    if last and minute > last:
        aggregate_5m(token, last)
    last_minute_seen[token] = minute

def aggregate_5m(token, closed_min):
    five = closed_min.replace(minute=(closed_min.minute // 5) * 5)
    key = (token, five)
    if key in candles_5m:
        return

    parts = [candles_1m.get((token, five + timedelta(minutes=i))) for i in range(5)]
    if any(p is None for p in parts):
        return

    candle = {
        "start": five,
        "open": parts[0]["open"],
        "high": max(p["high"] for p in parts),
        "low": min(p["low"] for p in parts),
        "close": parts[-1]["close"],
        "volume": sum(p["volume"] for p in parts)
    }

    candles_5m[key] = candle
    logger.info(f"5M CLOSED | token={token} | {five}")

    update_vwap(token, candle)
    evaluate_strategies(token, candle)
    check_vwap_recross_exit(token, candle)

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

# ============================================================
# STRATEGY ENGINE
# ============================================================

def evaluate_strategies(token, candle):
    if STRATEGIES["VWAP_ORB"]["enabled"]:
        evaluate_orb(token, candle)

    if STRATEGIES["VWAP_CROSSOVER"]["enabled"]:
        evaluate_vwap_crossover(token, candle)

def evaluate_orb(token, candle):
    if token not in opening_range or not opening_range[token]["finalized"]:
        return

    close = candle["close"]
    orr = opening_range[token]
    vwap = vwap_state[token]["vwap"]

    if close > orr["high"] and close > vwap:
        paper_enter_position(token, "LONG", candle, "VWAP_ORB")
    elif close < orr["low"] and close < vwap:
        paper_enter_position(token, "SHORT", candle, "VWAP_ORB")

def evaluate_vwap_crossover(token, candle):
    prev = candles_5m.get((token, candle["start"] - timedelta(minutes=5)))
    if not prev:
        return

    vwap = vwap_state[token]["vwap"]
    if prev["close"] < vwap and candle["close"] > vwap:
        paper_enter_position(token, "LONG", candle, "VWAP_CROSSOVER")
    elif prev["close"] > vwap and candle["close"] < vwap:
        paper_enter_position(token, "SHORT", candle, "VWAP_CROSSOVER")

# ============================================================
# EXECUTION — PAPER
# ============================================================

def paper_enter_position(token, direction, candle, strategy):
    if token in positions and positions[token]["open"]:
        return

    positions[token] = {
        "direction": direction,
        "entry_price": candle["close"],
        "open": True,
        "strategy": strategy
    }

    logger.info(
        f"PAPER ENTRY | {strategy} | token={token} | {direction} @ {candle['close']}"
    )

def paper_exit_position(token, price, reason):
    pos = positions.get(token)
    if not pos or not pos["open"]:
        return

    pos["open"] = False
    logger.info(
        f"PAPER EXIT | {pos['strategy']} | token={token} | {reason}"
    )

def check_vwap_recross_exit(token, candle):
    pos = positions.get(token)
    if not pos or not pos["open"]:
        return

    vwap = vwap_state[token]["vwap"]
    if pos["direction"] == "LONG" and candle["close"] < vwap:
        paper_exit_position(token, candle["close"], "VWAP_RECROSS")
    elif pos["direction"] == "SHORT" and candle["close"] > vwap:
        paper_exit_position(token, candle["close"], "VWAP_RECROSS")

# ============================================================
# HEARTBEAT
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
        for t in ticks:
            process_tick_to_1m(t)

    kws.on_connect = on_connect
    kws.on_ticks = on_ticks
    kws.connect(threaded=True)

# ============================================================
# BOOTSTRAP THREAD
# ============================================================

def start_background_engine():
    bootstrap_checks()

    kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
    kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN"))

    nifty = resolve_current_month_fut(kite, "NIFTY")
    banknifty = resolve_current_month_fut(kite, "BANKNIFTY")

    token_meta[nifty] = {"index": "NIFTY"}
    token_meta[banknifty] = {"index": "BANKNIFTY"}

    logger.info("Attempting VWAP backfill")
    backfill_vwap(kite, nifty)
    backfill_vwap(kite, banknifty)

    logger.info("Attempting Opening Range backfill")
    backfill_opening_range(kite, nifty)
    backfill_opening_range(kite, banknifty)

    start_kite_ticker([nifty, banknifty])
    logger.info("BACKGROUND ENGINE STARTED")

# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    threading.Thread(target=start_background_engine, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
