"""
strategies/orb.py

VWAP + Opening Range Breakout (ORB) strategy.

Entry Logic:
- Opening Range must be finalized
- Candle closes ABOVE OR high AND ABOVE VWAP → LONG
- Candle closes BELOW OR low AND BELOW VWAP → SHORT
- Per-direction trade limits enforced per strategy × index × day

Exit Logic:
- Handled by risk / execution engine (not here)

This module MUST:
- Not place trades
- Not manage positions
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
# INTERNAL HELPERS
# ============================================================

def get_trade_limit(config, *, base_key, index, strategy):
    """
    Resolves trade limits using strict precedence:

    1. base_key_index_strategy
    2. base_key_strategy
    3. base_key_index
    4. base_key
    5. default = 1
    """
    keys = []

    if index and strategy:
        keys.append(f"{base_key}_{index.lower()}_{strategy.lower()}")
    if strategy:
        keys.append(f"{base_key}_{strategy.lower()}")
    if index:
        keys.append(f"{base_key}_{index.lower()}")

    keys.append(base_key)

    for k in keys:
        if k in config:
            return int(config[k])

    return 1

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
    assert "max_trades_per_day" in " ".join(config.keys()), \
        "Trade limits missing from STRATEGY_CONFIG"
    
    # --- Safety checks ---
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

    # --- Reset per IST day ---
    if strat_state["date"] != today:
        strat_state["date"] = today
        strat_state["LONG"] = 0
        strat_state["SHORT"] = 0

    close = candle["close"]
    vwap = vwap_state[token]["vwap"]
    or_high = or_state["high"]
    or_low = or_state["low"]

    signal = None

    # --- LONG breakout ---
    if close > or_high and close > vwap:
        max_long = _get_trade_limit(
            config, "max_trades_per_day_long", index
        )
        if strat_state["LONG"] < max_long:
            strat_state["LONG"] += 1
            signal = "LONG"

    # --- SHORT breakout ---
    elif close < or_low and close < vwap:
        max_short = _get_trade_limit(
            config, "max_trades_per_day_short", index
        )
        if strat_state["SHORT"] < max_short:
            strat_state["SHORT"] += 1
            signal = "SHORT"

    if not signal:
        return None

    logger.info(
        f"ORB SIGNAL | token={token} | index={index} | "
        f"{signal} | close={close} | "
        f"OR=({or_low},{or_high}) | VWAP={round(vwap,2)}"
    )

    return {
        "strategy": STRATEGY_NAME,
        "token": token,
        "direction": signal,
        "price": close,
        "time": candle["start"]
    }
