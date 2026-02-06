"""
execution/router.py

Routes strategy signals to LIVE or PAPER execution engines
based on per-strategy × index configuration.
"""

import logging

logger = logging.getLogger(__name__)

def route_signal(
    signal,
    token_meta,
    execution_config,
    *,
    paper_engine,
    live_engine,
    live_trading_enabled
):
    """
    Decide how (or whether) to execute a signal.

    signal = {
        "strategy": str,
        "token": int,
        "direction": "LONG" | "SHORT",
        "price": float,
        "time": datetime
    }
    """

    strategy = signal["strategy"]
    token = signal["token"]
    index = token_meta.get(token, {}).get("index")

    if not index:
        logger.warning(f"EXEC SKIP | token={token} | missing index")
        return False

    cfg = execution_config.get((strategy, index))

    if not cfg:
        logger.info(
            f"EXEC SKIP | {strategy} | {index} | no execution config"
        )
        return False

    if not cfg.get("enabled", False):
        logger.info(
            f"EXEC SKIP | {strategy} | {index} | disabled"
        )
        return False

    mode = cfg["mode"]
    qty = cfg["qty"]

    if mode == "OFF":
        return False

    if mode == "PAPER":
        return paper_engine.enter_position(
            token=token,
            signal=signal,
            qty=qty
        )

    if mode == "LIVE":
        if not live_trading_enabled:
            logger.warning(
                f"LIVE BLOCKED | {strategy} | {index} | global safety off"
            )
            return False

        return live_engine.enter_position(
            token=token,
            signal=signal,
            qty=qty
        )

    logger.error(
        f"EXEC ERROR | {strategy} | {index} | unknown mode={mode}"
    )
    return False
