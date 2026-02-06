"""
execution/option_resolver.py

Resolves option contracts for live execution.

Responsibilities:
- ATM & shifted strikes
- Expiry filtering
- Avoid strike collisions across strategies
"""
# execution/option_resolver.py

from datetime import date

# ============================================================
# STRIKE STEP DISCOVERY
# ============================================================

def get_strike_step(index, instrument_cache):
    strikes = sorted({
        i["strike"]
        for i in instrument_cache
        if i["segment"] == "NFO-OPT"
        and i["name"] == index
    })

    if len(strikes) < 2:
        raise RuntimeError(f"Cannot determine strike step for {index}")

    return strikes[1] - strikes[0]


# ============================================================
# OPTION RESOLUTION
# ============================================================

def resolve_option(
    *,
    index,
    direction,
    kite_client,
    instrument_cache,
    positions,
    min_expiry_days
):
    """
    Resolve an option instrument respecting:
    - ATM proximity
    - Strike conflicts with other strategies
    - Minimum expiry days
    """

    ltp_symbol = f"NSE:{index}"
    ltp = kite_client.ltp(ltp_symbol)[ltp_symbol]["last_price"]

    step = get_strike_step(index, instrument_cache)
    atm = round(ltp / step) * step

    used_strikes = {
        pos["strike"]
        for pos in positions.values()
        if pos["index"] == index
        and pos["direction"] == direction
        and pos["open"]
    }

    option_type = "CE" if direction == "LONG" else "PE"

    candidates = [
        i for i in instrument_cache
        if i["segment"] == "NFO-OPT"
        and i["name"] == index
        and i["instrument_type"] == option_type
        and (i["expiry"] - date.today()).days >= min_expiry_days
    ]

    # Closest expiry first, then closest strike
    candidates.sort(key=lambda x: (x["expiry"], abs(x["strike"] - atm)))

    for c in candidates:
        if c["strike"] not in used_strikes:
            return c

    return None
