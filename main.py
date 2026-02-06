"""
TRKD — Algorithmic Trading Runtime (Modular)

Responsibilities:
- Bootstrap dependencies (Kite + Google Sheets)
- Run Track A (live ticks → 1m → 5m)
- Run Track B (backfill indicators)
- Route candles to strategies
- Execute paper trades and exit checks
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
from engine.config_loader import get_strategy_config, get_execution_config
from engine.strategy_router import route_strategies
from execution.paper import enter_position, exit_position
from execution.router import route_signal
from risk.exits import evaluate_exits
from state import token_meta, vwap_state, opening_range, positions, strategy_state
from performance.tracker import record_signal, update_option_marks

# ============================================================
# INSTRUMENT RESOLUTION (ATM OPTIONS)
# ============================================================

INDEX_LTP_SYMBOL = {
    "NIFTY": "NSE:NIFTY 50",
    "BANKNIFTY": "NSE:NIFTY BANK"
}


def resolve_atm_option(symbol, direction):
    if not kite_client or not instrument_cache:
        return None

    ltp_symbol = INDEX_LTP_SYMBOL.get(symbol)
    if not ltp_symbol:
        return None

    ltp = kite_client.ltp(ltp_symbol).get(ltp_symbol, {}).get("last_price")
    if not ltp:
        return None

    option_type = "CE" if direction == "LONG" else "PE"

    candidates = [
        i for i in instrument_cache
        if i.get("segment") == "NFO-OPT"
        and i.get("name") == symbol
        and i.get("instrument_type") == option_type
    ]
    if not candidates:
        return None

    candidates.sort(key=lambda x: (x["expiry"], abs(x["strike"] - ltp)))
    return candidates[0]

def get_atm_option_ltp(index, direction):
    option = resolve_atm_option(index, direction)
    if not option:
        return None, None

    trading_symbol = option.get("tradingsymbol")
    if not trading_symbol:
        return None, None

    ltp_key = f"NFO:{trading_symbol}"
    ltp = kite_client.ltp(ltp_key).get(ltp_key, {}).get("last_price")
    if ltp is None:
        return None, None

    return trading_symbol, ltp


def log_atm_option_price(signal):
    index = token_meta.get(signal["token"], {}).get("index")
    if not index:
        return None, None

    trading_symbol, ltp = get_atm_option_ltp(index, signal["direction"])
    if not trading_symbol or ltp is None:
        return None, None

    logger.info(
        f"ATM OPTION | index={index} | "
        f"signal={signal['direction']} | "
        f"symbol={trading_symbol} | "
        f"ltp={ltp}"
    )
    return trading_symbol, ltp
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


@app.route("/reload-config", methods=["POST"])
def reload_config():
    global strategy_config, execution_config
    strategy_config = get_strategy_config(
        gspread_client, os.getenv("GOOGLE_SHEET_ID"), force_reload=True
    )
    execution_config = get_execution_config(
        gspread_client, os.getenv("GOOGLE_SHEET_ID"), force_reload=True
    )
    return jsonify({"status": "reloaded", "strategies": list(strategy_config.keys())})


# ============================================================
# SHARED STATE
# ============================================================

gspread_client = None
kite_client = None
instrument_cache = None
strategy_config = {}
execution_config = {}
LIVE_TRADING_ENABLED = True


class PaperEngine:
    @staticmethod
    def enter_position(*, token, signal, qty):
        return enter_position(positions, token, signal, qty)


class LiveEngine:
    @staticmethod
    def enter_position(*, token, signal, qty):
        logger.info(
            f"LIVE PLACEHOLDER | token={token} | strategy={signal['strategy']} | qty={qty}"
        )
        return False

# ============================================================
# BOOTSTRAP
# ============================================================


def bootstrap_checks():
    logger.info("=== BOOTSTRAP START ===")

    for k in ["KITE_API_KEY", "KITE_API_SECRET", "KITE_ACCESS_TOKEN", "GOOGLE_SHEET_ID"]:
        logger.info(f"Secret {k} present: {os.getenv(k) is not None}")

    creds, _ = default()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.getenv("GOOGLE_SHEET_ID"))

    logger.info(f"SYSTEM_CONTROL rows: {len(sh.worksheet('SYSTEM_CONTROL').get_all_records())}")
    logger.info(f"STRATEGY_CONFIG rows: {len(sh.worksheet('STRATEGY_CONFIG').get_all_records())}")

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
        f"SELECTED FUT → {index} | {sel['tradingsymbol']} | "
        f"Expiry={sel['expiry']} | Token={sel['instrument_token']}"
    )
    return sel["instrument_token"]


# ============================================================
# TRACK A — TICKS → CANDLES
# ============================================================


def on_minute_close(token, closed_minute):
    candle_5m = aggregate_5m(token, closed_minute)
    if not candle_5m:
        return

    update_vwap_from_candle(token, candle_5m, vwap_state)
    update_opening_range_from_candle(token, candle_5m, opening_range)

    global strategy_config, execution_config
    strategy_config = get_strategy_config(
        gspread_client, os.getenv("GOOGLE_SHEET_ID")
    )
    execution_config = get_execution_config(
        gspread_client, os.getenv("GOOGLE_SHEET_ID")
    )

    prev_candle = candles_5m.get((token, candle_5m["start"] - timedelta(minutes=5)))

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

    for signal in signals:
        signal["index"] = token_meta.get(signal["token"], {}).get("index")
        option_symbol, option_ltp = log_atm_option_price(signal)
        if option_symbol and option_ltp is not None:
            record_signal(
                strategy=signal["strategy"],
                index=token_meta.get(signal["token"], {}).get("index"),
                direction=signal["direction"],
                option_symbol=option_symbol,
                ltp=option_ltp,
                qty=1
            )
        route_signal(
            signal,
            token_meta,
            execution_config,
            paper_engine=PaperEngine(),
            live_engine=LiveEngine(),
            live_trading_enabled=LIVE_TRADING_ENABLED
        )

    if kite_client:
        update_option_marks(kite_client)

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
# BOOTSTRAP THREAD
# ============================================================


def start_background_engine():
    global gspread_client, strategy_config, execution_config, kite_client, instrument_cache

    gspread_client = bootstrap_checks()

    kite_client = KiteConnect(os.getenv("KITE_API_KEY"))
    kite_client.set_access_token(os.getenv("KITE_ACCESS_TOKEN"))

    instrument_cache = kite_client.instruments("NFO")
    nifty = resolve_current_month_fut(kite_client, "NIFTY")
    banknifty = resolve_current_month_fut(kite_client, "BANKNIFTY")

    token_meta[nifty] = {"index": "NIFTY"}
    token_meta[banknifty] = {"index": "BANKNIFTY"}

    backfill_vwap(kite_client, nifty, vwap_state)
    backfill_vwap(kite_client, banknifty, vwap_state)

    backfill_opening_range(kite_client, nifty, opening_range)
    backfill_opening_range(kite_client, banknifty, opening_range)

    strategy_config = get_strategy_config(
        gspread_client, os.getenv("GOOGLE_SHEET_ID"), force_reload=True
    )
    execution_config = get_execution_config(
        gspread_client, os.getenv("GOOGLE_SHEET_ID"), force_reload=True
    )

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
