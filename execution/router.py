"""
execution/router.py

Routes strategy signals to LIVE or PAPER execution engines
based on per-strategy × index configuration.
"""

import logging
from execution.option_resolver import resolve_option

logger = logging.getLogger(__name__)


def route_signal(
    signal,
    token_meta,
    execution_config,
    *,
    paper_engine,
    live_engine,
    live_trading_enabled,
    kite_client,
    instrument_cache,
    positions
):
    strategy = signal["strategy"]
    token = signal["token"]
    direction = signal["direction"]

    index = token_meta.get(token, {}).get("index")
    if not index:
        return

    exec_cfg = execution_config.get((strategy, index))
    if not exec_cfg or not exec_cfg.get("enabled"):
        return

    mode = exec_cfg["mode"]
    qty = exec_cfg["qty"]
    min_expiry_days = exec_cfg.get("min_expiry_days", 7)

    # =========================
    # PAPER MODE
    # =========================
    if mode == "PAPER":
        paper_engine.enter_position(
            token=token,
            signal=signal,
            qty=qty
        )
        return

    # =========================
    # LIVE MODE
    # =========================
    if mode == "LIVE" and live_trading_enabled:
        option = resolve_option(
            index=index,
            direction=direction,
            kite_client=kite_client,
            instrument_cache=instrument_cache,
            positions=positions,
            min_expiry_days=min_expiry_days
        )

        if not option:
            logger.warning(
                f"NO OPTION AVAILABLE | {strategy} | {index} | {direction}"
            )
            return

        live_engine.enter_position(
            token=token,
            signal=signal,
            qty=qty,
            option=option
        )
