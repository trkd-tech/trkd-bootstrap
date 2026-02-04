"""
strategies/orb.py

VWAP + Opening Range Breakout (ORB) strategy.

Entry Logic:
- Opening Range must be finalized
- Candle closes ABOVE OR high AND ABOVE VWAP → LONG
- Candle closes BELOW OR low AND BELOW VWAP → SHORT
- Max trades per day enforced per token

Exit Logic:
- Handled by risk / execution engine (not here)

This module MUST:
- Not place real trades
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
#     "trades": int
# }

# ============================================================
# CORE EVALUATION
# ============================================================

def evaluate_orb(
    token,
    candle,
    vwap_state,
    opening_range,
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

    # --- Safety checks ---
    if token not in opening_range:
        return None

    or_state = opening_range[token]
    if not or_state.get("finalized"):
        return None

    if token not in vwap_state or vwap_state[token].get("vwap") is None:
        return None

    today = candle["start"].date()

    token_state = strategy_state.setdefault(token, {})
    strat_state = token_state.setdefault(STRATEGY_NAME, {
        "date": today,
        "trades": 0
    })

    # Reset per new day
    if strat_state["date"] != today:
        strat_state["date"] = today
        strat_state["trades"] = 0

    max_trades = config.get("max_trades_per_day", 1)
    if strat_state["trades"] >= max_trades:
        return None

    close = candle["close"]
    vwap = vwap_state[token]["vwap"]
    or_high = or_state["high"]
    or_low = or_state["low"]

    signal = None

    if close > or_high and close > vwap:
        signal = "LONG"
    elif close < or_low and close < vwap:
        signal = "SHORT"

    if not signal:
        return None

    strat_state["trades"] += 1

    logger.info(
        f"ORB SIGNAL | token={token} | "
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
