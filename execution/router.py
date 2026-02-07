# execution/router.py

import logging

logger = logging.getLogger(__name__)


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
    """

    index = token_meta.get(signal["token"], {}).get("index")
    if not index:
        logger.warning("EXEC SKIP | unknown index")
        return

    exec_key = (signal["strategy"], index)
    cfg = execution_config.get(exec_key)

    if not cfg or not cfg.get("enabled", False):
        logger.info("EXEC SKIP | execution disabled")
        return

    mode = cfg.get("mode")
    qty = cfg.get("qty", 0)

    if qty <= 0 or mode == "OFF":
        return

    if mode == "LIVE":
        if not live_trading_enabled:
            logger.warning("LIVE BLOCKED | system kill switch")
            return

        # option must be resolved BEFORE this call
        option = signal.get("option")
        if not option:
            logger.warning("LIVE SKIP | no option resolved")
            return

        live_engine.enter_position(
            signal=signal,
            qty=qty,
            option=option
        )

    elif mode == "PAPER":
        paper_engine.enter_position(
            token=signal["token"],
            signal=signal,
            qty=qty
        )
