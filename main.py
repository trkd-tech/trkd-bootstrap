"""
TRKD — Algorithmic Trading Runtime (Monolith v1.1)

Strategies implemented:
1. VWAP Opening Range Breakout (ORB)
2. VWAP Crossover (5-min)

Execution:
- Paper trading only (LIVE switch guarded)

Architecture:
- Single-file by design (stability > purity)
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
# DATA ENGINE STATE
# ------------------------------------------------------------

candles_1m = {}        # (token, minute_start) -> candle
candles_5m = {}        # (token, five_min_start) -> candle
last_minute_seen = {}  # token -> last minute timestamp

# ------------------------------------------------------------
# INDICATORS STATE
# ------------------------------------------------------------

vwap_state = {}        # token -> {cum_pv, cum_vol, vwap}
opening_range = {}    # token -> {high, low, finalized}

# ------------------------------------------------------------
# STRATEGY STATE (per strategy, per token)
# ------------------------------------------------------------

STRATEGY_ORB = "VWAP_ORB"
STRATEGY_VWAP_CROSS = "VWAP_CROSS"

strategy_state = {
    STRATEGY_ORB: {},         # token -> {triggered, date}
    STRATEGY_VWAP_CROSS: {}   # token -> {long_count, short_count, date}
}

# ------------------------------------------------------------
# EXECUTION STATE
# ------------------------------------------------------------

positions = {}  # token -> position dict

PAPER_QTY = {
    "NIFTY": 50,
    "BANKNIFTY": 15
}

# ------------------------------------------------------------
# RISK CONFIG
# ------------------------------------------------------------

TRAIL_SL_POINTS = {
    "NIFTY": 40,
    "BANKNIFTY": 120
}

TIME_EXIT_HHMM = "15:20"

# ------------------------------------------------------------
# VWAP CROSSOVER CONFIG (Strategy #2)
# ------------------------------------------------------------

VWAP_CROSS_CONFIG = {
    "direction": "BOTH",            # LONG / SHORT / BOTH
    "max_trades_per_side": 1,       # per day
    "trade_after": "09:45",         # cannot be before 09:45
    "trade_before": "14:45",        # entries stop after this
    "timeframe": "5m"
}

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# FLASK (Cloud Run requirement)
# ============================================================

app = Flask(__name__)

@app.route("/")
def health_check():
    return "TRKD runtime alive", 200

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
# DATA ENGINE — TICKS → 1M
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
        aggregate_5m_from_1m(token, last)
    last_minute_seen[token] = minute

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
        f"L={candle['low']} C={candle['close']}"
    )

    update_vwap(token, candle)
    update_opening_range(token, candle)

    evaluate_orb_breakout(token, candle)
    evaluate_vwap_crossover(token, candle)

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

    logger.info(
        f"VWAP | token={token} | "
        f"upto={candle['start']} | VWAP={round(s['vwap'],2)}"
    )

def update_opening_range(token, candle):
    t = candle["start"].time()
    if not (datetime.strptime("09:15","%H:%M").time()
            <= t < datetime.strptime("09:45","%H:%M").time()):
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
        logger.info(
            f"OPENING RANGE FINALIZED | token={token} | "
            f"H={s['high']} L={s['low']}"
        )

# ============================================================
# STRATEGY 1 — VWAP ORB
# ============================================================

def evaluate_orb_breakout(token, candle):
    today = candle["start"].date()
    state = strategy_state[STRATEGY_ORB].get(token)

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

    logger.info(
        f"ORB CHECK | token={token} | "
        f"close={close} OR=({orr['low']},{orr['high']}) VWAP={round(vwap,2)}"
    )

    if signal:
        strategy_state[STRATEGY_ORB][token] = {
            "triggered": True,
            "date": today
        }
        paper_enter_position(token, signal, candle, STRATEGY_ORB)

# ============================================================
# STRATEGY 2 — VWAP CROSSOVER
# ============================================================

def evaluate_vwap_crossover(token, candle):
    cfg = VWAP_CROSS_CONFIG
    today = candle["start"].date()

    state = strategy_state[STRATEGY_VWAP_CROSS].setdefault(token, {
        "long_count": 0,
        "short_count": 0,
        "date": today
    })

    if state["date"] != today:
        state.update({"long_count": 0, "short_count": 0, "date": today})

    t = candle["start"].time()
    if t < datetime.strptime("09:45","%H:%M").time():
        return
    if t < datetime.strptime(cfg["trade_after"],"%H:%M").time():
        return
    if t > datetime.strptime(cfg["trade_before"],"%H:%M").time():
        return

    prev_key = (token, candle["start"] - timedelta(minutes=5))
    prev = candles_5m.get(prev_key)
    vw = vwap_state.get(token)

    if not prev or not vw:
        return

    prev_close = prev["close"]
    close = candle["close"]
    vwap = vw["vwap"]

    logger.info(
        f"VWAP_CROSS CHECK | token={token} | "
        f"prev={prev_close} curr={close} VWAP={round(vwap,2)}"
    )

    # LONG crossover
    if (
        cfg["direction"] in ("LONG","BOTH")
        and prev_close < vwap
        and close > vwap
        and state["long_count"] < cfg["max_trades_per_side"]
    ):
        state["long_count"] += 1
        paper_enter_position(token, "LONG", candle, STRATEGY_VWAP_CROSS)

    # SHORT crossover
    if (
        cfg["direction"] in ("SHORT","BOTH")
        and prev_close > vwap
        and close < vwap
        and state["short_count"] < cfg["max_trades_per_side"]
    ):
        state["short_count"] += 1
        paper_enter_position(token, "SHORT", candle, STRATEGY_VWAP_CROSS)

# ============================================================
# EXECUTION — PAPER
# ============================================================

def paper_enter_position(token, signal, candle, strategy):
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
        "best_price": price,
        "strategy": strategy
    }

    logger.info(
        f"PAPER ENTRY | STRATEGY={strategy} | "
        f"token={token} | {signal} @ {price}"
    )

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
    logger.info(
        f"PAPER EXIT | STRATEGY={pos['strategy']} | "
        f"token={token} | {reason} | PNL={round(pnl,2)}"
    )

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
        for tick in ticks:
            try:
                process_tick_to_1m(tick)
                check_trailing_sl(tick)
                check_time_exit(tick)
            except Exception:
                logger.exception("Tick error (non-fatal)")

    kws.on_connect = on_connect
    kws.on_ticks = on_ticks
    kws.on_close = lambda ws, code, reason: logger.warning(
        f"Kite WebSocket closed: {code} {reason}"
    )

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
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    threading.Thread(target=start_background_engine, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
