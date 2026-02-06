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
- Per-direction trade limits enforced per strategy × index × day

Exit Logic:
- Reverse VWAP crossover (handled elsewhere)
- Time-based exits handled elsewhere
- Trailing SL handled elsewhere

This module MUST:
- Not place trades
- Not manage positions
- Only emit strategy signals
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
# INTERNAL HELPERS
# ============================================================

def _get_trade_limit(config, base_key, index):
    """
    Resolve per-strategy × index trade limit.

    Priority:
    1. max_trades_per_day_<long|short>_<index>_<strategy>
    2. max_trades_per_day_<long|short>_<index>
    3. max_trades_per_day_<long|short>
    4. default = 1
    """
    if index:
        index = index.lower()
        strategy = STRATEGY_NAME.lower()

        k1 = f"{base_key}_{index}_{strategy}"
        if k1 in config:
            return int(config[k1])

        k2 = f"{base_key}_{index}"
        if k2 in config:
            return int(config[k2])

    return int(config.get(base_key, 1))

# ============================================================
# CORE EVALUATION
# ============================================================

def evaluate_vwap_crossover(
    *,
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

    index = token_meta.get(token, {}).get("index")
    if not index:
        return None

    # --- Time filters ---
    t = candle["start"].time()

    trade_after = datetime.strptime(
        config.get("trade_after", "09:45"), "%H:%M"
    ).time()
    trade_before = datetime.strptime(
        config.get("trade_before", "15:00"), "%H:%M"
    ).time()

    if t < HARD_START or t < trade_after or t > trade_before:
        return None

    today = candle["start"].date()

    token_state = strategy_state.setdefault(token, {})
    strat_state = token_state.setdefault(
        STRATEGY_NAME,
        {"date": today, "LONG": 0, "SHORT": 0}
    )

    # --- Reset per IST day ---
    if strat_state["date"] != today:
        strat_state["date"] = today
        strat_state["LONG"] = 0
        strat_state["SHORT"] = 0

    prev_close = prev_candle["close"]
    close = candle["close"]
    vwap = vwap_state[token]["vwap"]

    allowed_dir = config.get("direction", "BOTH")

    max_long = _get_trade_limit(
        config, "max_trades_per_day_long", index
    )
    max_short = _get_trade_limit(
        config, "max_trades_per_day_short", index
    )

    signal = None

    # --- LONG crossover ---
    if (
        prev_close < vwap and
        close > vwap and
        allowed_dir in ("UP", "BOTH") and
        strat_state["LONG"] < max_long
    ):
        strat_state["LONG"] += 1
        signal = "LONG"

    # --- SHORT crossover ---
    elif (
        prev_close > vwap and
        close < vwap and
        allowed_dir in ("DOWN", "BOTH") and
        strat_state["SHORT"] < max_short
    ):
        strat_state["SHORT"] += 1
        signal = "SHORT"

    if not signal:
        return None

    logger.info(
        f"VWAP CROSS SIGNAL | token={token} | index={index} | "
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
