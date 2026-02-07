# execution/live.py

import logging
from datetime import date

logger = logging.getLogger(__name__)


class LiveEngine:
    """
    Hardened live execution engine.

    Responsibilities:
    - Place live orders via Kite
    - Track positions keyed by (strategy, instrument_token)
    - Sync with Kite before any action
    """

    def __init__(self, kite_client, positions):
        self.kite = kite_client
        self.positions = positions

    # ---------------------------------------------------------
    # SYNC WITH KITE (CRITICAL)
    # ---------------------------------------------------------

    def sync_positions_from_kite(self):
        """
        Reconcile internal positions with Kite net positions.
        Marks positions closed if Kite no longer shows them.
        """
        kite_positions = self.kite.positions().get("net", [])

        live_tokens = {
            p["instrument_token"]: p
            for p in kite_positions
            if p.get("quantity", 0) != 0
        }

        for key, pos in self.positions.items():
            token = pos["instrument_token"]
            if pos["open"] and token not in live_tokens:
                pos["open"] = False
                logger.warning(
                    f"LIVE DESYNC | {pos['tradingsymbol']} marked CLOSED (not in Kite)"
                )

    # ---------------------------------------------------------
    # ENTRY
    # ---------------------------------------------------------

    def enter_position(self, *, signal, qty, option):
        """
        Place a live order for an option instrument.

        option must contain:
        - tradingsymbol
        - instrument_token
        - strike
        - expiry
        - name (index)
        """

        self.sync_positions_from_kite()

        instrument_token = option["instrument_token"]
        key = (signal["strategy"], instrument_token)

        if key in self.positions and self.positions[key]["open"]:
            logger.warning("LIVE ENTRY BLOCKED | position already open")
            return False

        tradingsymbol = option["tradingsymbol"]

        txn_type = (
            self.kite.TRANSACTION_TYPE_BUY
            if signal["direction"] == "LONG"
            else self.kite.TRANSACTION_TYPE_SELL
        )

        order_id = self.kite.place_order(
            variety=self.kite.VARIETY_REGULAR,
            exchange="NFO",
            tradingsymbol=tradingsymbol,
            transaction_type=txn_type,
            quantity=qty,
            product=self.kite.PRODUCT_NRML,
            order_type=self.kite.ORDER_TYPE_MARKET
        )

        self.positions[key] = {
            "strategy": signal["strategy"],
            "index": option["name"],
            "direction": signal["direction"],
            "tradingsymbol": tradingsymbol,
            "instrument_token": instrument_token,
            "strike": option["strike"],
            "expiry": option["expiry"],
            "qty": qty,
            "open": True,
            "kite_order_id": order_id,
            "entry_date": date.today()
        }

        logger.info(
            f"LIVE ENTRY | {signal['strategy']} | "
            f"{tradingsymbol} | qty={qty} | order_id={order_id}"
        )
        return True

    # ---------------------------------------------------------
    # EXIT
    # ---------------------------------------------------------

    def exit_position(self, *, instrument_token, qty, reason):
        """
        Exit an open live position after validating it still exists.
        """
        self.sync_positions_from_kite()

        for (strategy, token), pos in self.positions.items():
            if token != instrument_token or not pos["open"]:
                continue

            txn_type = (
                self.kite.TRANSACTION_TYPE_SELL
                if pos["direction"] == "LONG"
                else self.kite.TRANSACTION_TYPE_BUY
            )

            order_id = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                exchange="NFO",
                tradingsymbol=pos["tradingsymbol"],
                transaction_type=txn_type,
                quantity=qty,
                product=self.kite.PRODUCT_NRML,
                order_type=self.kite.ORDER_TYPE_MARKET
            )

            pos["open"] = False

            logger.info(
                f"LIVE EXIT | {strategy} | {pos['tradingsymbol']} | "
                f"reason={reason} | order_id={order_id}"
            )
            return True

        logger.warning("LIVE EXIT FAILED | no open position found")
        return False
