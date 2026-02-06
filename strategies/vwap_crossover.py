"""
strategies/vwap_crossover.py

VWAP Crossover strategy.

Entry Logic (5-minute candles):
- Candle closes ABOVE VWAP and previous candle closed BELOW VWAP → LONG
- Candle closes BELOW VWAP and previous candle closed ABOVE VWAP → SHORT
- Time filters:
    - Hard floor: not before 09:45
    - Must be >= trade_after (config)
    - Must be <= trade_before (config)
- Direction filter: UP / DOWN / BOTH
- Per-day trade limits enforced per strategy × index × direction

Exit Logic:
- Reverse VWAP crossover (handled elsewhere)
- Time exits handled by risk engine
- Trailing SL handled by risk engine

This module MUST:
- Not place trades
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

    # --------------------------------------------------------
    # SAFETY CHECKS
    # --------------------------------------------------------

    if not prev_candle:
        return None

    # Ensure sequential 5-minute candles
    if candle["start"] - prev_candle["start"] != timedelta(minutes=5):
        return None

    if token not in vwap_state or vwap_state[token].get("vwap") is None:
        return None

    index = token_meta.get(token, {}).get("index")
    if not index:
        return None

    t = candle["start"].time()

    trade_after = datetime.strptime(
        config.get("trade_after", "09:45"), "%H:%M"
    ).time()

    trade_before = datetime.strptime(
        config.get("trade_before", "15:00"), "%H:%M"
    ).time()

    # --------------------------------------------------------
    # TIME FILTERS
    # --------------------------------------------------------

    if t < HARD_START or t < trade_after or t > trade_before:
        return None

    today = candle["start"].date()

    token_state = strategy_state.setdefault(token, {})
    strat_state = token_state.setdefault(
        STRATEGY_NAME,
        {"date": today, "LONG": 0, "SHORT": 0}
    )

    # --------------------------------------------------------
    # DAILY RESET
    # --------------------------------------------------------

    if strat_state["date"] != today:
        strat_state["date"] = today
        strat_state["LONG"] = 0
        strat_state["SHORT"] = 0

    # --------------------------------------------------------
    # TRADE LIMITS (strategy × index × direction)
    # --------------------------------------------------------

    max_long = _get_trade_limit(
        config,
        base_key="max_trades_per_day_long",
        index=index,
        strategy=STRATEGY_NAME
    )

    max_short = _get_trade_limit(
        config,
        base_key="max_trades_per_day_short",
        index=index,
        strategy=STRATEGY_NAME
    )

    allowed_dir = config.get("direction", "BOTH")

    prev_close = prev_candle["close"]
    close = candle["close"]
    vwap = vwap_state[token]["vwap"]

    signal = None

    # --------------------------------------------------------
    # SIGNAL LOGIC
    # --------------------------------------------------------

    # --- LONG crossover ---
    if (
        prev_close < vwap and
        close > vwap and
        allowed_dir in ("UP", "BOTH") and
        strat_state["LONG"] < max_long
    ):
        signal = "LONG"
        strat_state["LONG"] += 1

    # --- SHORT crossover ---
    elif (
        prev_close > vwap and
        close < vwap and
        allowed_dir in ("DOWN", "BOTH") and
        strat_state["SHORT"] < max_short
    ):
        signal = "SHORT"
        strat_state["SHORT"] += 1

    if not signal:
        return None

    logger.info(
        f"VWAP CROSS SIGNAL | token={token} | index={index} | "
        f"{signal} | close={close} | "
        f"VWAP={round(vwap, 2)} | time={t}"
    )

    return {
        "strategy": STRATEGY_NAME,
        "token": token,
        "direction": signal,
        "price": close,
        "time": candle["start"]
    }

# ============================================================
# HELPERS
# ============================================================

def _get_trade_limit(config, base_key, index, strategy):
    """
    Resolve trade limits in the following priority order:

    1. max_trades_per_day_<direction>_<index>_<strategy>
       e.g. max_trades_per_day_long_nifty_vwap_crossover

    2. max_trades_per_day_<direction>_<strategy>
       e.g. max_trades_per_day_long_vwap_crossover

    3. max_trades_per_day_<direction>
       e.g. max_trades_per_day_long

    4. Default = 1
    """

    index = index.lower()
    strategy = strategy.lower()

    keys = [
        f"{base_key}_{index}_{strategy}",
        f"{base_key}_{strategy}",
        f"{base_key}",
    ]

    for key in keys:
        if key in config:
            try:
                return int(config[key])
            except (TypeError, ValueError):
                pass

    return 1
