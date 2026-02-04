"""
indicators/opening_range.py

Opening Range (OR) indicator.

Responsibilities:
- Maintain Opening Range high / low
- Handle OR window timing (09:15–09:45 IST)
- Finalize OR exactly once per session

This module MUST:
- Not place trades
- Not evaluate strategies
- Not fetch historical data
"""

import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ============================================================
# IST CONFIG
# ============================================================

IST_OFFSET = timedelta(hours=5, minutes=30)
IST = timezone(IST_OFFSET)

OR_START = datetime.strptime("09:15", "%H:%M").time()
OR_END   = datetime.strptime("09:45", "%H:%M").time()

# ============================================================
# OPENING RANGE STATE SHAPE
# ============================================================
# opening_range[token] = {
#     "high": float,
#     "low": float,
#     "finalized": bool
# }

# ============================================================
# CORE UPDATE
# ============================================================

def update_opening_range_from_candle(token, candle, opening_range):
    """
    Update Opening Range using a completed candle.

    Candle must include:
    {
        "start": datetime (IST),
        "high": float,
        "low": float
    }

    Mutates opening_range[token]
    """

    candle_time = candle["start"].time()

    # Ignore candles outside OR window
    if candle_time < OR_START or candle_time >= OR_END:
        return

    state = opening_range.setdefault(token, {
        "high": candle["high"],
        "low": candle["low"],
        "finalized": False
    })

    if state["finalized"]:
        return

    state["high"] = max(state["high"], candle["high"])
    state["low"] = min(state["low"], candle["low"])

    # Finalize at the last OR candle close (09:40–09:45 bucket)
    if candle_time >= datetime.strptime("09:40", "%H:%M").time():
        state["finalized"] = True
        logger.info(
            f"OPENING RANGE FINALIZED | token={token} | "
            f"H={state['high']} L={state['low']}"
        )

# ============================================================
# READ HELPERS
# ============================================================

def is_or_finalized(token, opening_range):
    """
    Returns True if Opening Range is finalized for token.
    """
    return (
        token in opening_range and
        opening_range[token].get("finalized") is True
    )


def get_opening_range(token, opening_range):
    """
    Safe getter for Opening Range.
    Returns None if not ready.
    """
    if not is_or_finalized(token, opening_range):
        return None
    return opening_range[token]


# ============================================================
# RESET (SESSION BOUNDARY)
# ============================================================

def reset_opening_range(token, opening_range):
    """
    Reset Opening Range state for a token.
    Intended for session boundary handling (future).
    """
    if token in opening_range:
        del opening_range[token]
        logger.info(f"OPENING RANGE RESET | token={token}")
