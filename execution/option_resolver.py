# execution/option_resolver.py

from datetime import timedelta, date
import logging

logger = logging.getLogger(__name__)


def resolve_option_for_signal(
    *,
    kite,
    instrument_cache,
    signal,
    positions,
    token_meta,
    config
):
    """
    Resolve an option instrument for a signal:
    - ATM base
    - Shift strike if conflict
    - Enforce min expiry days
    """

    index = token_meta.get(signal["token"], {}).get("index")
    if not index:
        return None

    min_expiry_days = int(config.get("min_expiry_days", 7))
    direction = signal["direction"]

    # --- Fetch index LTP ---
    ltp_symbol = f"NSE:{index}"
    ltp = kite.ltp(ltp_symbol)[ltp_symbol]["last_price"]

    # --- Collect used strikes ---
    used_strikes = {
        pos["strike"]
        for pos in positions.values()
        if pos["index"] == index and pos["open"]
    }

    # --- Candidate options ---
    candidates = [
        i for i in instrument_cache
        if i["segment"] == "NFO-OPT"
        and i["name"] == index
        and i["expiry"] >= date.today() + timedelta(days=min_expiry_days)
        and (
            (direction == "LONG" and i["instrument_type"] == "CE") or
            (direction == "SHORT" and i["instrument_type"] == "PE")
        )
    ]

    if not candidates:
        logger.warning("NO OPTIONS | expiry constraint failed")
        return None

    # --- Sort by expiry then ATM distance ---
    candidates.sort(
        key=lambda x: (x["expiry"], abs(x["strike"] - ltp))
    )

    for opt in candidates:
        if opt["strike"] not in used_strikes:
            return opt

    logger.warning("NO FREE STRIKE | all strikes occupied")
    return None
