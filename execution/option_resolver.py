"""
execution/option_resolver.py

Resolves option contracts for live execution.

Responsibilities:
- ATM & shifted strikes
- Expiry filtering
- Avoid strike collisions across strategies
"""

import logging
from datetime import timedelta

logger = logging.getLogger(__name__)

STRIKE_STEP = {
    "NIFTY": 50,
    "BANKNIFTY": 100
}


def resolve_option(
    *,
    index,
    direction,
    kite_client,
    instrument_cache,
    open_positions,
    min_expiry_days,
):
    """
    Returns option instrument dict or None.
    """

    # --- Get index LTP ---
    spot_symbol = f"NSE:{index}"
    ltp = kite_client.ltp(spot_symbol).get(spot_symbol, {}).get("last_price")
    if not ltp:
        logger.warning(f"OPTION RESOLVE FAILED | no LTP | {index}")
        return None

    step = STRIKE_STEP[index]
    atm_strike = round(ltp / step) * step

    # --- Determine shift count ---
    shift = 0
    for pos in open_positions.values():
        if pos["index"] == index and pos["direction"] == direction:
            shift += 1

    strike = atm_strike + (shift * step if direction == "LONG" else -shift * step)

    option_type = "CE" if direction == "LONG" else "PE"

    # --- Filter instruments ---
    candidates = [
        i for i in instrument_cache
        if i["segment"] == "NFO-OPT"
        and i["name"] == index
        and i["instrument_type"] == option_type
        and i["strike"] == strike
        and (i["expiry"] - i["expiry"].__class__.today()).days >= min_expiry_days
    ]

    if not candidates:
        logger.warning(
            f"OPTION RESOLVE FAILED | {index} {direction} | strike={strike}"
        )
        return None

    candidates.sort(key=lambda x: x["expiry"])
    return candidates[0]
