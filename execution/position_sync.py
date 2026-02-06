# execution/position_sync.py

import logging

logger = logging.getLogger(__name__)


def sync_positions_from_kite(kite_client, positions):
    """
    Sync runtime positions with Kite positions.

    Kite is the source of truth.
    If a runtime position is no longer present at Kite → mark closed.
    """

    try:
        kite_positions = kite_client.positions()["net"]
    except Exception:
        logger.exception("FAILED TO FETCH KITE POSITIONS")
        return

    kite_open = {
        p["tradingsymbol"]: p
        for p in kite_positions
        if p["quantity"] != 0
    }

    for key, pos in list(positions.items()):
        if not pos.get("open"):
            continue

        ts = pos.get("tradingsymbol")

        if ts not in kite_open:
            pos["open"] = False
            pos["exit_reason"] = "EXTERNAL_CLOSE"

            logger.warning(
                f"POSITION CLOSED EXTERNALLY | "
                f"{pos['strategy']} | {ts}"
            )
