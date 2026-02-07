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
    - Reconcile state with Kite before every action
    """

    def __init__(self, kite_client, positions):
        self.kite = kite_client
        self.positions = positions

    # =========================================================
    # SYNC WITH KITE (CRITICAL SAFETY)
    # =========================================================

    def sync_positions_from_kite(self):
        """
        Reconcile internal positions with Kite net positions.

        If a position is no longer present in Kite, mark it closed
        internally to avoid ghost trades.
        """
        try:
            kite_positions = self.kite.positions().get("net", [])
        except Exception:
            logger.exception("KITE POSITION FETCH FAILED")
            return

        live_tokens = {
            p["instrument_token"]: p
            for p in kite_positions
            if p.get("quantity", 0) != 0
        }

        for key, pos in self.positions.items():
            token = pos["instrument_token"]

            if pos["open"] and token not in live_tokens:
                pos["open"] = False
                pos["closed_externally"] = True

                logger.warning(
                    f"LIVE DESYNC | {pos['tradingsymbol']} "
                    f"marked CLOSED (not found in Kite)"
                )

    # =========================================================
    # ENTRY
    # =========================================================

    def enter_position(self, *, token, signal, qty, option, exec_config=None):
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

        # Prevent duplicate live positions
        if key in self.positions and self.positions[key].get("open"):
            logger.warning(
                f"LIVE ENTRY BLOCKED | already open | {option['tradingsymbol']}"
            )
            return False

        tradingsymbol = option["tradingsymbol"]

        txn_type = (
            self.kite.TRANSACTION_TYPE_BUY
            if signal["direction"] == "LONG"
            else self.kite.TRANSACTION_TYPE_SELL
        )

        try:
            order_id = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                exchange="NFO",
                tradingsymbol=tradingsymbol,
                transaction_type=txn_type,
                quantity=qty,
                product=self.kite.PRODUCT_NRML,
                order_type=self.kite.ORDER_TYPE_MARKET
            )
        except Exception:
            logger.exception(
                f"LIVE ENTRY FAILED | {signal['strategy']} | {tradingsymbol}"
            )
            return False

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
            "entry_price": signal.get("price"),
            "entry_time": signal.get("time"),
            "entry_date": date.today(),
            "best_price": signal.get("price"),  # for trailing SL
            "closed_externally": False
        }

        logger.info(
            f"LIVE ENTRY | {signal['strategy']} | "
            f"{tradingsymbol} | qty={qty} | order_id={order_id}"
        )
        return True

    # =========================================================
    # EXIT
    # =========================================================

    def exit_position(self, *, instrument_token, qty, reason):
        """
        Exit an open live position after validating it still exists.
        Supports partial or full exits.
        """

        self.sync_positions_from_kite()

        for (strategy, token), pos in self.positions.items():
            if token != instrument_token:
                continue

            if not pos.get("open"):
                logger.warning(
                    f"LIVE EXIT SKIPPED | already closed | {pos['tradingsymbol']}"
                )
                return False

            txn_type = (
                self.kite.TRANSACTION_TYPE_SELL
                if pos["direction"] == "LONG"
                else self.kite.TRANSACTION_TYPE_BUY
            )

            try:
                order_id = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange="NFO",
                    tradingsymbol=pos["tradingsymbol"],
                    transaction_type=txn_type,
                    quantity=qty,
                    product=self.kite.PRODUCT_NRML,
                    order_type=self.kite.ORDER_TYPE_MARKET
                )
            except Exception:
                logger.exception(
                    f"LIVE EXIT FAILED | {pos['tradingsymbol']}"
                )
                return False

            # Full exit
            if qty >= pos["qty"]:
                pos["open"] = False
                pos["exit_reason"] = reason
            else:
                pos["qty"] -= qty  # partial exit

            logger.info(
                f"LIVE EXIT | {strategy} | {pos['tradingsymbol']} | "
                f"qty={qty} | reason={reason} | order_id={order_id}"
            )
            return True

        logger.warning(
            f"LIVE EXIT FAILED | instrument_token={instrument_token} | no open position"
        )
        return False
