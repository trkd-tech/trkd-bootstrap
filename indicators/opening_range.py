"""
opening_range.py

Indicator: Opening Range (OR)

Responsibilities:
- Build opening range using 5-minute candles
- Finalize OR after the OR window
- Provide safe read-only access to OR state

This module:
- Does NOT fetch historical data
- Does NOT evaluate strategies
- Does NOT place trades
"""

from datetime import datetime

# ============================================================
# CONFIG
# ============================================================

OR_START = datetime.strptime("09:15", "%H:%M").time()
OR_END   = datetime.strptime("09:45", "%H:%M").time()

# ============================================================
# OR UPDATE LOGIC
# ============================================================

def update_opening_range(opening_range, token, candle):
    """
    Update Opening Range using a closed candle.

    Expected candle timeframe: 5-minute
    Expected candle structure:
    {
        "start": datetime,
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": int
    }
    """

    candle_time = candle["start"].time()

    # Ignore candles outside OR window
    if not (OR_START <= candle_time < OR_END):
        return

    state = opening_range.setdefault(
        token,
        {
            "high": candle["high"],
            "low": candle["low"],
            "finalized": False
        }
    )

    if state["finalized"]:
        return

    state["high"] = max(state["high"], candle["high"])
    state["low"] = min(state["low"], candle["low"])

    # Finalize OR on 09:40 candle close (completes at 09:45)
    if candle_time == datetime.strptime("09:40", "%H:%M").time():
        state["finalized"] = True


# ============================================================
# READ-ONLY ACCESSORS
# ============================================================

def is_or_finalized(opening_range, token):
    state = opening_range.get(token)
    return bool(state and state.get("finalized"))


def get_opening_range(opening_range, token):
    """
    Returns:
        (low, high) | None
    """
    state = opening_range.get(token)
    if not state or not state.get("finalized"):
        return None
    return state["low"], state["high"]
