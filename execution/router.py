# execution/router.py

import logging

logger = logging.getLogger(__name__)

# ============================================================
# EXECUTION ROUTER
# ============================================================

def route_signal(
    signal,
    *,
    token_meta,
    execution_config,
    paper_engine,
    live_engine,
    live_trading_enabled
):
    """
    Decide how (or if) a signal is executed.

    Execution decision is made using:
        (strategy, index, direction)

    signal = {
        "strategy": str,
        "token": int,
        "direction": "LONG" | "SHORT",
        "price": float,
        "time": datetime,
        "option": dict | None   # required for LIVE
    }
    """

    token = signal.get("token")
    strategy = signal.get("strategy")
    direction = signal.get("direction")

    if not all([token, strategy, direction]):
        logger.warning("EXEC SKIP | malformed signal")
        return

    index = token_meta.get(token, {}).get("index")
    if not index:
        logger.warning("EXEC SKIP | unknown index")
        return

    exec_key = (strategy, index, direction)
    cfg = execution_config.get(exec_key)

    if not cfg:
        logger.info(f"EXEC SKIP | no execution config | key={exec_key}")
        return

    if not cfg.get("enabled", False):
        logger.info(f"EXEC SKIP | disabled | key={exec_key}")
        return

    mode = cfg.get("mode")
    qty = cfg.get("qty", 0)

    if mode == "OFF" or qty <= 0:
        logger.info(f"EXEC SKIP | mode={mode} qty={qty}")
        return

    # ========================================================
    # LIVE EXECUTION
    # ========================================================

    if mode == "LIVE":
        if not live_trading_enabled:
            logger.warning("LIVE BLOCKED | system kill switch active")
            return

        option = signal.get("option")
        if not option:
            logger.warning("LIVE SKIP | option not resolved")
            return

        live_engine.enter_position(
            token=token,
            signal=signal,
            qty=qty,
            option=option,
            exec_config=cfg
        )
        return

    # ========================================================
    # PAPER EXECUTION
    # ========================================================

    if mode == "PAPER":
        paper_engine.enter_position(
            token=token,
            signal=signal,
            qty=qty
        )
        return

    logger.warning(f"EXEC SKIP | unknown mode={mode}")
