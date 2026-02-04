"""
risk/exits.py

Risk & exit engine.

Responsibilities:
- Trailing stop-loss
- Time-based exit (hard square-off)
- Strategy-neutral exits

This module MUST:
- Never enter positions
- Never compute indicators
- Never generate signals
"""

from datetime import time
import logging

logger = logging.getLogger(__name__)

# ============================================================
# CONFIG (safe defaults)
# ============================================================

TIME_EXIT = time(15, 20)   # 3:20 PM IST hard exit

# trailing_sl_points[index] = points
TRAILING_SL_POINTS = {
    "NIFTY": 40,
    "BANKNIFTY": 120
}

# ============================================================
# TRAILING STOP LOSS
# ============================================================

def check_trailing_sl(
    positions,
    token,
    ltp,
    index_name
):
    """
    Apply trailing SL to an open position.

    Assumes:
    - best_price is tracked inside position
    """

    pos = positions.get(token)
    if not pos or not pos["open"]:
        return None

    trail = TRAILING_SL_POINTS.get(index_name)
    if not trail:
        return None

    # Initialise best price
    pos.setdefault("best_price", pos["entry_price"])

    if pos["direction"] == "LONG":
        pos["best_price"] = max(pos["best_price"], ltp)

        if pos["best_price"] - ltp >= trail:
            return {
                "token": token,
                "price": ltp,
                "reason": "TRAILING_SL"
            }

    else:  # SHORT
        pos["best_price"] = min(pos["best_price"], ltp)

        if ltp - pos["best_price"] >= trail:
            return {
                "token": token,
                "price": ltp,
                "reason": "TRAILING_SL"
            }

    return None

# ============================================================
# TIME EXIT
# ============================================================

def check_time_exit(
    positions,
    token,
    candle_time,
    price
):
    """
    Force exit after TIME_EXIT.
    """

    pos = positions.get(token)
    if not pos or not pos["open"]:
        return None

    if candle_time.time() >= TIME_EXIT:
        return {
            "token": token,
            "price": price,
            "reason": "TIME_EXIT"
        }

    return None
