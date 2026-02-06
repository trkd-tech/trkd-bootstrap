"""
strategies/orb.py

VWAP + Opening Range Breakout (ORB) strategy.

Entry Logic:
- Opening Range must be finalized
- Candle closes ABOVE OR high AND ABOVE VWAP → LONG
- Candle closes BELOW OR low AND BELOW VWAP → SHORT
- Per-day trade limits enforced per strategy × index × direction

Exit Logic:
- Handled by risk / execution engine (not here)

This module MUST:
- Not place trades
- Only emit strategy signals
"""

import logging
from datetime import date

logger = logging.getLogger(__name__)

STRATEGY_NAME = "VWAP_ORB"

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

def evaluate_orb(
    *,
    token,
    candle,
    vwap_state,
    opening_range,
    token_meta,
    strategy_state,
    config
):
    """
    Evaluate ORB conditions on a completed candle.

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

    if token not in opening_range:
        return None

    or_state = opening_range[token]
    if not or_state.get("finalized"):
        return None

    if token not in vwap_state or vwap_state[token].get("vwap") is None:
        return None

    index = token_meta.get(token, {}).get("index")
    if not index:
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

    close = candle["close"]
    vwap = vwap_state[token]["vwap"]
    or_high = or_state["high"]
    or_low = or_state["low"]

    signal = None

    # --------------------------------------------------------
    # SIGNAL LOGIC
    # --------------------------------------------------------

    if close > or_high and close > vwap:
        if strat_state["LONG"] < max_long:
            signal = "LONG"
            strat_state["LONG"] += 1

    elif close < or_low and close < vwap:
        if strat_state["SHORT"] < max_short:
            signal = "SHORT"
            strat_state["SHORT"] += 1

    if not signal:
        return None

    logger.info(
        f"ORB SIGNAL | token={token} | index={index} | "
        f"{signal} | close={close} | "
        f"OR=({or_low},{or_high}) | VWAP={round(vwap, 2)}"
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
       e.g. max_trades_per_day_long_nifty_vwap_orb

    2. max_trades_per_day_<direction>_<strategy>
       e.g. max_trades_per_day_long_vwap_orb

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
