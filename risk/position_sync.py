"""
risk/position_sync.py

Validates open positions against Kite.
"""

import logging

logger = logging.getLogger(__name__)


def is_position_open(kite_client, position):
    """
    Confirms if a position still exists on Kite.
    """

    try:
        kite_positions = kite_client.positions()["net"]

        for kp in kite_positions:
            if (
                kp["tradingsymbol"] == position["tradingsymbol"]
                and kp["quantity"] != 0
            ):
                return True

        return False

    except Exception:
        logger.exception("POSITION SYNC FAILED")
        return False
