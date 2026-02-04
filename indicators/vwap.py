"""
vwap.py

Indicator: VWAP (Volume Weighted Average Price)

Responsibilities:
- Maintain session VWAP state
- Extend VWAP using closed candles (Track A)
- Start from backfilled state if present (Track B)

This module:
- Does NOT fetch historical data
- Does NOT evaluate strategies
- Does NOT place trades
"""

# ============================================================
# VWAP UPDATE LOGIC
# ============================================================

def update_vwap_from_candle(vwap_state, token, candle):
    """
    Update VWAP using a closed candle.

    Args:
        vwap_state (dict): global vwap_state
        token (int): instrument token
        candle (dict): closed candle with high, low, close, volume

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

    # Typical price for the candle
    typical_price = (
        candle["high"] +
        candle["low"] +
        candle["close"]
    ) / 3

    pv = typical_price * candle["volume"]

    state = vwap_state.setdefault(
        token,
        {
            "cum_pv": 0.0,
            "cum_vol": 0,
            "vwap": None
        }
    )

    state["cum_pv"] += pv
    state["cum_vol"] += candle["volume"]

    # Guard against zero volume
    if state["cum_vol"] > 0:
        state["vwap"] = state["cum_pv"] / state["cum_vol"]

    return state["vwap"]


# ============================================================
# READ-ONLY ACCESSOR
# ============================================================

def get_vwap(vwap_state, token):
    """
    Safe accessor for current VWAP.

    Returns:
        float | None
    """
    state = vwap_state.get(token)
    if not state:
        return None
    return state.get("vwap")
