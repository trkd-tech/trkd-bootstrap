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

def resolve_option(
    *,
    index,
    direction,
    kite_client,
    instrument_cache,
    positions,
    min_expiry_days
):
    ltp_symbol = f"NSE:{index}"
    ltp = kite_client.ltp(ltp_symbol)[ltp_symbol]["last_price"]

    step = get_strike_step(index, instrument_cache)
    atm = round(ltp / step) * step

    used_strikes = {
        p["strike"]
        for p in positions.values()
        if p["index"] == index and p["direction"] == direction and p["open"]
    }

    option_type = "CE" if direction == "LONG" else "PE"

    candidates = [
        i for i in instrument_cache
        if i["segment"] == "NFO-OPT"
        and i["name"] == index
        and i["instrument_type"] == option_type
        and (i["expiry"] - date.today()).days >= min_expiry_days
    ]

    candidates.sort(key=lambda x: (x["expiry"], abs(x["strike"] - atm)))

    for c in candidates:
        if c["strike"] not in used_strikes:
            return c

    return None
