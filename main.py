"""
TRKD — Algorithmic Trading Runtime (Modular, Locked)

Responsibilities:
- Bootstrap dependencies (Kite + Google Sheets)
- Run Track A (live ticks → 1m → 5m)
- Run Track B (VWAP + OR backfill)
- Route completed candles to strategies
- Execute paper trades
- Evaluate exits

This file MUST:
- Not contain strategy logic
- Not compute indicators
- Not place live trades
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

# =========================
# DATA / TRACK A
# =========================

from data.ticks import start_kite_ticker, process_tick_to_1m
from data.candles import (
    candles_1m,
    candles_5m,
    last_minute_seen,
    aggregate_5m,
)

# =========================
# TRACK B — BACKFILL
# =========================

from data.backfill import backfill_vwap, backfill_opening_range

# =========================
# INDICATORS
# =========================

from indicators.vwap import update_vwap_from_candle
from indicators.opening_range import update_opening_range_from_candle

# =========================
# ENGINE
# =========================

from engine.config_loader import get_strategy_config
from engine.strategy_router import route_strategies

# =========================
# EXECUTION
# =========================

from execution.paper import enter_position, exit_position

# =========================
# RISK
# =========================

from risk.exits import evaluate_exits

# =========================
# SHARED STATE
# =========================

from state import (
    token_meta,
    vwap_state,
    opening_range,
    positions,
    strategy_state,
)

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# FLASK (Health + Reload)
# ============================================================

app = Flask(__name__)

@app.route("/")
def health():
    return "TRKD alive", 200

@app.route("/reload-config", methods=["POST"])
def reload_config():
    global strategy_config
    strategy_config = get_strategy_config(
        gspread_client,
        os.getenv("GOOGLE_SHEET_ID"),
        force_reload=True
    )
    return jsonify({
        "status": "reloaded",
        "strategies": list(strategy_config.keys())
    })

# ============================================================
# GLOBALS (BOOTSTRAP-ONLY MUTATION)
# ============================================================

gspread_client = None
kite_client = None
strategy_config = {}

# ============================================================
# BOOTSTRAP
# ============================================================

def bootstrap_checks():
    logger.info("=== BOOTSTRAP START ===")

    required = [
        "KITE_API_KEY",
        "KITE_ACCESS_TOKEN",
        "GOOGLE_SHEET_ID"
    ]

    for k in required:
        assert os.getenv(k), f"Missing env var: {k}"
        logger.info(f"ENV OK | {k}")

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
    sel = candidates[0]

    logger.info(
        f"SELECTED FUT → {index} | "
        f"{sel['tradingsymbol']} | "
        f"Expiry={sel['expiry']} | "
        f"Token={sel['instrument_token']}"
    )
    return sel["instrument_token"]

# ============================================================
# TRACK A CALLBACK — MINUTE CLOSE
# ============================================================

def on_minute_close(token, closed_minute):
    """
    Called exactly once per closed 1-minute candle.
    Aggregates to 5-minute and triggers strategy evaluation.
    """

    candle_5m = aggregate_5m(token, closed_minute)
    if not candle_5m:
        return

    # --- Indicators ---
    update_vwap_from_candle(token, candle_5m, vwap_state)
    update_opening_range_from_candle(token, candle_5m, opening_range)

    # --- Strategy config (cached daily) ---
    global strategy_config
    strategy_config = get_strategy_config(
        gspread_client,
        os.getenv("GOOGLE_SHEET_ID")
    )

    prev_candle = candles_5m.get(
        (token, candle_5m["start"] - timedelta(minutes=5))
    )

    # --- HARD ARCHITECTURE ASSERTS ---
    assert token in token_meta, "token_meta missing token"
    assert token in vwap_state, "VWAP state missing"
    assert isinstance(strategy_config, dict), "Invalid strategy_config"

    # --- Strategy routing ---
    signals = route_strategies(
        token=token,
        candle=candle_5m,
        prev_candle=prev_candle,
        vwap_state=vwap_state,
        opening_range=opening_range,
        token_meta=token_meta,
        strategy_state=strategy_state,
        strategy_config=strategy_config
    )

    # --- Paper execution ---
    for signal in signals:
        route_signal(
            signal,
            token_meta,
            execution_config,
            paper_engine=PaperEngine(),
            live_engine=LiveEngine(),
            live_trading_enabled=LIVE_TRADING_ENABLED
        )

    # --- Risk / exits ---
    evaluate_exits(
        token=token,
        candle=candle_5m,
        vwap_state=vwap_state,
        positions=positions,
        token_meta=token_meta,
        exit_position=exit_position
    )

# ============================================================
# HEARTBEAT
# ============================================================

def heartbeat():
    while True:
        logger.info("SYSTEM ALIVE | waiting for ticks")
        time.sleep(60)

# ============================================================
# BACKGROUND ENGINE
# ============================================================

def start_background_engine():
    global gspread_client, kite_client, strategy_config

    gspread_client = bootstrap_checks()

    kite_client = KiteConnect(os.getenv("KITE_API_KEY"))
    kite_client.set_access_token(os.getenv("KITE_ACCESS_TOKEN"))

    # Resolve instruments
    nifty = resolve_current_month_fut(kite_client, "NIFTY")
    banknifty = resolve_current_month_fut(kite_client, "BANKNIFTY")

    token_meta[nifty] = {"index": "NIFTY"}
    token_meta[banknifty] = {"index": "BANKNIFTY"}

    # --- Track B: Backfill ---
    backfill_vwap(kite_client, nifty, vwap_state)
    backfill_vwap(kite_client, banknifty, vwap_state)

    backfill_opening_range(kite_client, nifty, opening_range)
    backfill_opening_range(kite_client, banknifty, opening_range)

    # --- Load config once at boot ---
    strategy_config = get_strategy_config(
        gspread_client,
        os.getenv("GOOGLE_SHEET_ID"),
        force_reload=True
    )

    # --- Start ticks ---
    start_kite_ticker(
        api_key=os.getenv("KITE_API_KEY"),
        access_token=os.getenv("KITE_ACCESS_TOKEN"),
        tokens=[nifty, banknifty],
        on_tick_callback=lambda tick: process_tick_to_1m(
            tick,
            candles_1m,
            last_minute_seen,
            on_minute_close
        )
    )

    logger.info("BACKGROUND ENGINE STARTED")

# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    threading.Thread(target=start_background_engine, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
