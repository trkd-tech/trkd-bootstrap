"""
strategies/vwap_crossover.py

VWAP Crossover strategy.

Entry Logic (5-minute candles):
- Candle closes ABOVE VWAP and previous candle closed BELOW VWAP → LONG
- Candle closes BELOW VWAP and previous candle closed ABOVE VWAP → SHORT
- Time filter:
    - Not before 09:45 (hard floor)
    - Must be >= trade_after (user config)
    - Must be <= trade_before (user config)
- Direction filter: UP / DOWN / BOTH
- Max trades per direction per day

Exit Logic:
- Reverse VWAP crossover
- Time-based exits handled elsewhere
- Trailing SL handled elsewhere

This module MUST:
- Not place trades
- Only emit signals
"""

import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

STRATEGY_NAME = "VWAP_CROSSOVER"
HARD_START = datetime.strptime("09:45", "%H:%M").time()

# ============================================================
# STRATEGY STATE SHAPE
# ============================================================
# strategy_state[token][STRATEGY_NAME] = {
#     "date": date,
#     "LONG": int,
#     "SHORT": int
# }

# ============================================================
# CORE EVALUATION
# ============================================================

def evaluate_vwap_crossover(
    token,
    candle,
    prev_candle,
    vwap_state,
    token_meta,
    strategy_state,
    config
):
    """
    Evaluate VWAP crossover on a completed candle.

    Returns:
        None OR dict {
            "strategy": STRATEGY_NAME,
            "token": token,
            "direction": "LONG" | "SHORT",
            "price": candle["close"],
            "time": candle["start"]
        }
    """

    # --- Safety checks ---
    if not prev_candle:
        return None

    # Ensure candles are sequential (5-minute gap)
    if candle["start"] - prev_candle["start"] != timedelta(minutes=5):
        return None

    if token not in vwap_state or vwap_state[token].get("vwap") is None:
        return None

    t = candle["start"].time()

    trade_after = datetime.strptime(
        config.get("trade_after", "09:45"), "%H:%M"
    ).time()
    trade_before = datetime.strptime(
        config.get("trade_before", "15:00"), "%H:%M"
    ).time()

    # --- Time filters ---
    if t < HARD_START or t < trade_after or t > trade_before:
        return None

    today = candle["start"].date()

    token_state = strategy_state.setdefault(token, {})
    strat_state = token_state.setdefault(
        STRATEGY_NAME,
        {"date": today, "LONG": 0, "SHORT": 0}
    )

    # Reset per day
    if strat_state["date"] != today:
        strat_state["date"] = today
        strat_state["LONG"] = 0
        strat_state["SHORT"] = 0

    index = token_meta.get(token, {}).get("index")
    max_trades_long = _get_trade_limit(config, "max_trades_per_day_long", index, STRATEGY_NAME)
    max_trades_short = _get_trade_limit(config, "max_trades_per_day_short", index, STRATEGY_NAME)
    allowed_dir = config.get("direction", "BOTH")

    prev_close = prev_candle["close"]
    close = candle["close"]
    vwap = vwap_state[token]["vwap"]

    signal = None

    # --- LONG crossover ---
    if (
        prev_close < vwap and
        close > vwap and
        allowed_dir in ("UP", "BOTH") and
        strat_state["LONG"] < max_trades_long
    ):
        signal = "LONG"
        strat_state["LONG"] += 1

    # --- SHORT crossover ---
    elif (
        prev_close > vwap and
        close < vwap and
        allowed_dir in ("DOWN", "BOTH") and
        strat_state["SHORT"] < max_trades_short
    ):
        signal = "SHORT"
        strat_state["SHORT"] += 1

    if not signal:
        return None

    logger.info(
        f"VWAP CROSS SIGNAL | token={token} | "
        f"{signal} | close={close} | VWAP={round(vwap,2)} | "
        f"time={candle['start'].time()}"
    )

    return {
        "strategy": STRATEGY_NAME,
        "token": token,
        "direction": signal,
        "price": close,
        "time": candle["start"]
    }


def _get_trade_limit(config, base_key, index, strategy_name):
    strategy = strategy_name.lower()
    if index:
        index_key = index.lower()
        indexed_key = f"{base_key}_{index_key}_{strategy}"
        if indexed_key in config:
            return int(config.get(indexed_key, 0))
    return int(config.get(base_key, 1))
