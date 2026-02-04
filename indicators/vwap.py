"""
indicators/vwap.py

VWAP indicator logic.

Responsibilities:
- Maintain running VWAP state from candles
- Provide a single, consistent VWAP calculation method
- Be reusable by:
  - Track A (live 5m candles)
  - Track B (historical backfill)

This module MUST:
- Not know about strategies
- Not know about orders / execution
- Not fetch historical data
"""

import logging

logger = logging.getLogger(__name__)

# ============================================================
# VWAP STATE SHAPE
# ============================================================
# vwap_state[token] = {
#     "cum_pv": float,
#     "cum_vol": int,
#     "vwap": float
# }

# ============================================================
# CORE VWAP UPDATE
# ============================================================

def update_vwap_from_candle(token, candle, vwap_state):
    """
    Update VWAP using a single completed candle.

    Candle requirements:
    {
        "high": float,
        "low": float,
        "close": float,
        "volume": int
    }

    This function MUTATES vwap_state[token].
    """

    volume = candle.get("volume", 0)
    if volume <= 0:
        logger.debug(f"VWAP SKIP | token={token} | zero volume candle")
        return

    typical_price = (
        candle["high"] + candle["low"] + candle["close"]
    ) / 3.0

    pv = typical_price * volume

    state = vwap_state.setdefault(
        token,
        {"cum_pv": 0.0, "cum_vol": 0, "vwap": None}
    )

    state["cum_pv"] += pv
    state["cum_vol"] += volume
    state["vwap"] = state["cum_pv"] / state["cum_vol"]

    logger.debug(
        f"VWAP UPDATE | token={token} | "
        f"tp={round(typical_price,2)} | "
        f"vol={volume} | "
        f"vwap={round(state['vwap'],2)}"
    )


# ============================================================
# READ HELPERS
# ============================================================

def get_vwap(token, vwap_state):
    """
    Safe VWAP getter.
    Returns None if VWAP not available yet.
    """
    s = vwap_state.get(token)
    if not s:
        return None
    return s.get("vwap")


def has_vwap(token, vwap_state):
    """
    Returns True if VWAP is initialized for token.
    """
    return token in vwap_state and vwap_state[token].get("vwap") is not None


# ============================================================
# RESET (SESSION BOUNDARY)
# ============================================================

def reset_vwap(token, vwap_state):
    """
    Reset VWAP state for a token.
    Intended to be used at session boundary (future).
    """
    if token in vwap_state:
        del vwap_state[token]
        logger.info(f"VWAP RESET | token={token}")
