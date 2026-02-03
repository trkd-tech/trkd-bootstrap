"""
TRKD — Algorithmic Trading Runtime (Monolith v1)

Stability > purity.

Supports:
- Track A: Live ticks → 1m → 5m
- Track B: VWAP + Opening Range backfill
- Strategies: VWAP ORB, VWAP Crossover
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
# TIME CONSTANTS (IST – NAIVE)
# ============================================================

OR_START = datetime.strptime("09:15", "%H:%M").time()
OR_END   = datetime.strptime("09:45", "%H:%M").time()
TIME_EXIT_HHMM = "15:20"

# Cumulative volume tracker (required for VWAP correctness)
last_cum_volume = {}   # token -> last seen cumulative volume

# ============================================================
# CONFIG
# ============================================================

EXECUTION_MODE = "PAPER"
LIVE_TRADING_ENABLED = False

# ============================================================
# GLOBAL STATE
# ============================================================

token_meta = {}

candles_1m = {}
candles_5m = {}
last_minute_seen = {}

vwap_state = {}        # token -> {cum_pv, cum_vol, vwap, backfilled}
opening_range = {}    # token -> {high, low, finalized}

positions = {}
strategy_state = {}   # token -> strategy -> state

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
        "direction": "BOTH",
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
# FLASK
# ============================================================

app = Flask(__name__)

@app.route("/")
def health():
    return "TRKD alive", 200

# ============================================================
# BOOTSTRAP
# ============================================================

def bootstrap_checks():
    logger.info("=== BOOTSTRAP START ===")

    for k in ["KITE_API_KEY", "KITE_API_SECRET", "KITE_ACCESS_TOKEN"]:
        logger.info(f"Secret {k} present: {os.getenv(k) is not None}")

    creds, _ = default()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.getenv("GOOGLE_SHEET_ID"))

    logger.info(f"SYSTEM_CONTROL rows: {len(sh.worksheet('SYSTEM_CONTROL').get_all_records())}")
    logger.info(f"STRATEGIES rows: {len(sh.worksheet('STRATEGIES').get_all_records())}")

    logger.info("=== BOOTSTRAP SUCCESS ===")

# ============================================================
# INSTRUMENT RESOLUTION
# ============================================================

def resolve_current_month_fut(kite, index):
    instruments = kite.instruments("NFO")
    c = [
        i for i in instruments
        if i["segment"] == "NFO-FUT"
        and i["instrument_type"] == "FUT"
        and i["name"] == index
        and i["expiry"] >= date.today()
    ]
    c.sort(key=lambda x: x["expiry"])
    sel = c[0]

    logger.info(
        f"SELECTED FUT → {index} | {sel['tradingsymbol']} | "
        f"Expiry={sel['expiry']} | Token={sel['instrument_token']}"
    )
    return sel["instrument_token"]

# ============================================================
# TRACK B — BACKFILL
# ============================================================

def backfill_opening_range(kite, token):
    if opening_range.get(token, {}).get("finalized"):
        return

    today = date.today()
    candles = kite.historical_data(
        token,
        datetime.combine(today, OR_START),
        datetime.combine(today, OR_END),
        "5minute"
    )

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
        f"H={opening_range[token]['high']} "
        f"L={opening_range[token]['low']} | candles={len(candles)}"
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

    if not candles:
        logger.warning(f"VWAP BACKFILL FAILED | token={token}")
        return

    cum_pv, cum_vol = 0, 0
    for c in candles:
        tp = (c["high"] + c["low"] + c["close"]) / 3
        cum_pv += tp * c["volume"]
        cum_vol += c["volume"]

    if cum_vol == 0:
        return

    vwap_state[token] = {
        "cum_pv": cum_pv,
        "cum_vol": cum_vol,
        "vwap": cum_pv / cum_vol,
        "backfilled": True
    }

    logger.info(
        f"VWAP BACKFILL DONE | token={token} | "
        f"VWAP={round(vwap_state[token]['vwap'],2)} | candles={len(candles)}"
    )

# ============================================================
# TRACK A — TICKS → CANDLES
# ============================================================

def process_tick_to_1m(t):
    if "exchange_timestamp" not in t or "last_price" not in t:
        return

    token = t["instrument_token"]
    ts = t["exchange_timestamp"].replace(second=0, microsecond=0)
    price = t["last_price"]

    # --- FIX: convert cumulative volume → delta volume ---
    cum_vol = t.get("volume_traded")
    if cum_vol is None:
        return

    prev_cum = last_cum_volume.get(token)
    if prev_cum is None:
        delta_vol = 0
    else:
        delta_vol = max(cum_vol - prev_cum, 0)

    last_cum_volume[token] = cum_vol
    # -----------------------------------------------------

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
    c["volume"] += delta_vol   # ✅ CORRECT volume accumulation

    detect_minute_close(token, ts)


def detect_minute_close(token, minute):
    last = last_minute_seen.get(token)
    if last and minute > last:
        logger.info(f"1M CLOSED | token={token} | {last}")
        aggregate_5m(token, last)
    last_minute_seen[token] = minute

def aggregate_5m(token, m):
    five = m.replace(minute=(m.minute // 5) * 5)
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
    s = vwap_state.setdefault(token, {"cum_pv": 0, "cum_vol": 0, "vwap": None, "backfilled": False})
    tp = (candle["high"] + candle["low"] + candle["close"]) / 3
    s["cum_pv"] += tp * candle["volume"]
    s["cum_vol"] += candle["volume"]
    s["vwap"] = s["cum_pv"] / s["cum_vol"]

    logger.info(
        f"VWAP UPDATE | token={token} | "
        f"time={candle['start']} | "
        f"VWAP={round(s['vwap'], 2)}"
    )


# ============================================================
# STRATEGIES
# ============================================================

def evaluate_strategies(token, candle):
    if STRATEGIES["VWAP_ORB"]["enabled"]:
        evaluate_orb(token, candle)
    if STRATEGIES["VWAP_CROSSOVER"]["enabled"]:
        evaluate_vwap_crossover(token, candle)

def evaluate_orb(token, candle):
    if not opening_range.get(token, {}).get("finalized"):
        return

    state = strategy_state.setdefault(token, {}).setdefault("VWAP_ORB", {})
    if state.get("date") == candle["start"].date():
        return

    close = candle["close"]
    orr = opening_range[token]
    vwap = vwap_state[token]["vwap"]

    if close > orr["high"] and close > vwap:
        state["date"] = candle["start"].date()
        paper_enter_position(token, "LONG", candle, "VWAP_ORB")

    elif close < orr["low"] and close < vwap:
        state["date"] = candle["start"].date()
        paper_enter_position(token, "SHORT", candle, "VWAP_ORB")

def evaluate_vwap_crossover(token, candle):
    prev = candles_5m.get((token, candle["start"] - timedelta(minutes=5)))
    if not prev:
        return

    cfg = STRATEGIES["VWAP_CROSSOVER"]
    t = candle["start"].time()

    if not (datetime.strptime(cfg["trade_after"], "%H:%M").time() <= t <=
            datetime.strptime(cfg["trade_before"], "%H:%M").time()):
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
    if positions.get(token, {}).get("open"):
        return

    positions[token] = {
        "direction": direction,
        "entry_price": candle["close"],
        "open": True,
        "strategy": strategy
    }

    logger.info(f"PAPER ENTRY | {strategy} | token={token} | {direction} @ {candle['close']}")

def paper_exit_position(token, price, reason):
    pos = positions.get(token)
    if not pos or not pos["open"]:
        return
    pos["open"] = False
    logger.info(f"PAPER EXIT | {pos['strategy']} | token={token} | {reason}")

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
    kws = KiteTicker(os.getenv("KITE_API_KEY"), os.getenv("KITE_ACCESS_TOKEN"))

    kws.on_connect = lambda ws, r: (ws.subscribe(tokens), ws.set_mode(ws.MODE_FULL, tokens))
    kws.on_ticks = lambda ws, ticks: [process_tick_to_1m(t) for t in ticks]

    kws.connect(threaded=True)

# ============================================================
# BOOTSTRAP THREAD
# ============================================================

def start_background_engine():
    bootstrap_checks()

    kite = KiteConnect(os.getenv("KITE_API_KEY"))
    kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN"))

    nifty = resolve_current_month_fut(kite, "NIFTY")
    banknifty = resolve_current_month_fut(kite, "BANKNIFTY")

    token_meta[nifty] = {"index": "NIFTY"}
    token_meta[banknifty] = {"index": "BANKNIFTY"}

    backfill_vwap(kite, nifty)
    backfill_vwap(kite, banknifty)

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
