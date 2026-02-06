"""
TRKD — Algorithmic Trading Runtime (Modular)

Responsibilities:
- Bootstrap dependencies (Kite + Google Sheets)
- Run Track B (historical backfill)
- Run Track A (live ticks → candles)
- Route completed candles to strategies
- Route emitted signals to execution engine
- Evaluate exits

This file is ORCHESTRATION ONLY.
"""

import os
import logging
import threading
import time
from datetime import timedelta

from flask import Flask, jsonify
import gspread
from google.auth import default
from kiteconnect import KiteConnect

# =======================
# Internal imports
# =======================

from data.ticks import start_kite_ticker, process_tick_to_1m
from data.candles import (
    candles_1m,
    candles_5m,
    last_minute_seen,
    aggregate_5m,
)
from data.backfill import backfill_vwap, backfill_opening_range

from indicators.vwap import update_vwap_from_candle
from indicators.opening_range import update_opening_range_from_candle

from engine.config_loader import (
    get_strategy_config,
    get_execution_config,
    get_system_control,
)
from engine.strategy_router import route_strategies
from execution.router import route_signal
from execution.paper import enter_position, exit_position
from risk.exits import evaluate_exits

from state import (
    token_meta,
    vwap_state,
    opening_range,
    positions,
    strategy_state,
)

# =======================
# Logging
# =======================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =======================
# Flask (health + reload)
# =======================

app = Flask(__name__)

@app.route("/")
def health():
    return "TRKD alive", 200

@app.route("/reload-config", methods=["POST"])
def reload_config():
    global strategy_config, execution_config, LIVE_TRADING_ENABLED

    strategy_config = get_strategy_config(
        gspread_client, os.getenv("GOOGLE_SHEET_ID"), force_reload=True
    )
    execution_config = get_execution_config(
        gspread_client, os.getenv("GOOGLE_SHEET_ID"), force_reload=True
    )
    system_control = get_system_control(
        gspread_client, os.getenv("GOOGLE_SHEET_ID"), force_reload=True
    )

    if "LIVE_TRADING_ENABLED" in system_control:
        LIVE_TRADING_ENABLED = bool(system_control["LIVE_TRADING_ENABLED"])

    return jsonify({
        "status": "reloaded",
        "strategies": list(strategy_config.keys()),
        "live_trading": LIVE_TRADING_ENABLED,
    })

# =======================
# Global runtime state
# =======================

gspread_client = None
kite_client = None

strategy_config = {}
execution_config = {}
LIVE_TRADING_ENABLED = False

# =======================
# Bootstrap helpers
# =======================

def bootstrap_checks():
    logger.info("=== BOOTSTRAP START ===")

    for key in [
        "KITE_API_KEY",
        "KITE_API_SECRET",
        "KITE_ACCESS_TOKEN",
        "GOOGLE_SHEET_ID",
    ]:
        logger.info(f"Secret {key} present: {os.getenv(key) is not None}")

    creds, _ = default()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.getenv("GOOGLE_SHEET_ID"))

    logger.info(
        f"SYSTEM_CONTROL rows: "
        f"{len(sh.worksheet('SYSTEM_CONTROL').get_all_records())}"
    )
    logger.info(
        f"STRATEGY_CONFIG rows: "
        f"{len(sh.worksheet('STRATEGY_CONFIG').get_all_records())}"
    )

    logger.info("=== BOOTSTRAP SUCCESS ===")
    return gc

def resolve_current_month_fut(kite, index):
    instruments = kite.instruments("NFO")
    candidates = [
        i for i in instruments
        if i["segment"] == "NFO-FUT"
        and i["instrument_type"] == "FUT"
        and i["name"] == index
    ]
    candidates.sort(key=lambda x: x["expiry"])
    selected = candidates[0]

    logger.info(
        f"SELECTED FUT → {index} | "
        f"{selected['tradingsymbol']} | "
        f"Expiry={selected['expiry']} | "
        f"Token={selected['instrument_token']}"
    )
    return selected["instrument_token"]

# =======================
# Track A — candle close
# =======================

def on_minute_close(token, closed_minute):
    candle_5m = aggregate_5m(token, closed_minute)
    if not candle_5m:
        return

    # --- Invariants ---
    assert token in token_meta, "Unknown token"
    assert token in vwap_state, "VWAP state missing"

    # --- Indicators ---
    update_vwap_from_candle(token, candle_5m, vwap_state)
    update_opening_range_from_candle(token, candle_5m, opening_range)

    prev_candle = candles_5m.get(
        (token, candle_5m["start"] - timedelta(minutes=5))
    )

    # --- Strategy routing ---
    signals = route_strategies(
        token=token,
        candle=candle_5m,
        prev_candle=prev_candle,
        vwap_state=vwap_state,
        opening_range=opening_range,
        token_meta=token_meta,
        strategy_state=strategy_state,
        strategy_config=strategy_config,
    )

    # --- Execution routing ---
    for signal in signals:
        route_signal(
            signal=signal,
            token_meta=token_meta,
            execution_config=execution_config,
            positions=positions,
            enter_position=enter_position,
            live_trading_enabled=LIVE_TRADING_ENABLED,
        )

    # --- Exit checks ---
    evaluate_exits(
        token=token,
        candle=candle_5m,
        vwap_state=vwap_state,
        positions=positions,
        token_meta=token_meta,
        exit_position=exit_position,
    )

# =======================
# Heartbeat
# =======================

def heartbeat():
    while True:
        logger.info("SYSTEM ALIVE | waiting for ticks")
        time.sleep(60)

# =======================
# Bootstrap thread
# =======================

def start_background_engine():
    global gspread_client, kite_client
    global strategy_config, execution_config, LIVE_TRADING_ENABLED

    gspread_client = bootstrap_checks()

    kite_client = KiteConnect(os.getenv("KITE_API_KEY"))
    kite_client.set_access_token(os.getenv("KITE_ACCESS_TOKEN"))

    nifty = resolve_current_month_fut(kite_client, "NIFTY")
    banknifty = resolve_current_month_fut(kite_client, "BANKNIFTY")

    token_meta[nifty] = {"index": "NIFTY"}
    token_meta[banknifty] = {"index": "BANKNIFTY"}

    # --- Track B (backfill) ---
    backfill_vwap(kite_client, nifty, vwap_state)
    backfill_vwap(kite_client, banknifty, vwap_state)

    backfill_opening_range(kite_client, nifty, opening_range)
    backfill_opening_range(kite_client, banknifty, opening_range)

    # --- Load configs once ---
    strategy_config = get_strategy_config(
        gspread_client, os.getenv("GOOGLE_SHEET_ID"), force_reload=True
    )
    execution_config = get_execution_config(
        gspread_client, os.getenv("GOOGLE_SHEET_ID"), force_reload=True
    )
    system_control = get_system_control(
        gspread_client, os.getenv("GOOGLE_SHEET_ID"), force_reload=True
    )

    if "LIVE_TRADING_ENABLED" in system_control:
        LIVE_TRADING_ENABLED = bool(system_control["LIVE_TRADING_ENABLED"])

    # --- Start WebSocket ---
    start_kite_ticker(
        api_key=os.getenv("KITE_API_KEY"),
        access_token=os.getenv("KITE_ACCESS_TOKEN"),
        tokens=[nifty, banknifty],
        on_tick_callback=lambda tick: process_tick_to_1m(
            tick,
            candles_1m,
            last_minute_seen,
            on_minute_close,
        ),
    )

    logger.info("BACKGROUND ENGINE STARTED")

# =======================
# Entrypoint
# =======================

if __name__ == "__main__":
    threading.Thread(target=start_background_engine, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
