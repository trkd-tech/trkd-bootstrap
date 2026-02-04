"""
runtime.py

TRKD Runtime Orchestrator.

Responsibilities:
- Bootstrap system
- Resolve instruments
- Run Track B (backfill)
- Start WebSocket (Track A)
- Route completed candles to:
    - Indicators
    - Strategies
    - Execution
    - Risk exits

This file is the ONLY place where modules are connected.
"""

# ============================================================
# IMPORTS
# ============================================================

import os
import threading
import time
import logging
from datetime import datetime

from flask import Flask
from kiteconnect import KiteConnect, KiteTicker

# --- Internal modules ---
from data.candles import process_tick_to_1m, get_last_5m_candle
from data.backfill import backfill_vwap, backfill_opening_range
from strategies.strategy_router import route_strategies
from execution.paper import enter_position, exit_position
from risk.exits import evaluate_exits

# ============================================================
# GLOBAL STATE
# ============================================================

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

token_meta = {}        # token -> {"index": "NIFTY"/"BANKNIFTY"}
positions = {}         # paper positions
vwap_state = {}        # token -> vwap state
opening_range = {}     # token -> OR state
strategy_state = {}    # token -> per-strategy counters

# ============================================================
# FLASK (Cloud Run health check)
# ============================================================

app = Flask(__name__)

@app.route("/")
def health():
    return "TRKD runtime alive", 200

# ============================================================
# INSTRUMENT RESOLUTION
# ============================================================

def resolve_current_month_fut(kite, index_name):
    instruments = kite.instruments("NFO")

    candidates = [
        i for i in instruments
        if i["segment"] == "NFO-FUT"
        and i["instrument_type"] == "FUT"
        and i["name"] == index_name
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
# CANDLE CLOSE HANDLER (5-MIN)
# ============================================================

def on_5m_candle_close(token, candle):
    """
    Single choke point for every completed 5-minute candle.
    """

    # --------------------------------------------------------
    # 1. Strategy evaluation
    # --------------------------------------------------------
    signals = route_strategies(
        token=token,
        candle=candle,
        vwap_state=vwap_state,
        opening_range=opening_range,
        strategy_state=strategy_state
    )

    # --------------------------------------------------------
    # 2. Paper execution (entries)
    # --------------------------------------------------------
    for signal in signals:
        qty = 1  # placeholder (configurable later)
        enter_position(positions, token, signal, qty)

    # --------------------------------------------------------
    # 3. Risk exits (VWAP recross, trail SL, time exit)
    # --------------------------------------------------------
    evaluate_exits(
        token=token,
        candle=candle,
        vwap_state=vwap_state,
        positions=positions,
        token_meta=token_meta,
        exit_position=exit_position
    )

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
            process_tick_to_1m(
                tick=tick,
                on_5m_close=on_5m_candle_close,
                vwap_state=vwap_state
            )

    def on_close(ws, code, reason):
        logger.warning(f"Kite WS closed | code={code} | reason={reason}")

    kws.on_connect = on_connect
    kws.on_ticks = on_ticks
    kws.on_close = on_close

    kws.connect(threaded=True)

# ============================================================
# BOOTSTRAP
# ============================================================

def start_background_engine():
    logger.info("=== BOOTSTRAP START ===")

    kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
    kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN"))

    nifty = resolve_current_month_fut(kite, "NIFTY")
    banknifty = resolve_current_month_fut(kite, "BANKNIFTY")

    token_meta[nifty] = {"index": "NIFTY"}
    token_meta[banknifty] = {"index": "BANKNIFTY"}

    # --------------------------------------------------------
    # Track B — Historical backfill
    # --------------------------------------------------------
    logger.info("Attempting VWAP backfill")
    backfill_vwap(kite, nifty, vwap_state)
    backfill_vwap(kite, banknifty, vwap_state)

    logger.info("Attempting Opening Range backfill")
    backfill_opening_range(kite, nifty, opening_range)
    backfill_opening_range(kite, banknifty, opening_range)

    # --------------------------------------------------------
    # Track A — Live ticks
    # --------------------------------------------------------
    start_kite_ticker([nifty, banknifty])

    logger.info("BACKGROUND ENGINE STARTED")

# ============================================================
# HEARTBEAT
# ============================================================

def heartbeat():
    while True:
        logger.info("SYSTEM ALIVE | waiting for ticks")
        time.sleep(60)

# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    threading.Thread(target=start_background_engine, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()

    app.run(host="0.0.0.0", port=8080)
