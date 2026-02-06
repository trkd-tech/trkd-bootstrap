# execution/live.py

import logging
from execution.position_sync import sync_positions_from_kite

logger = logging.getLogger(__name__)


class LiveEngine:

    def __init__(self, kite_client, positions):
        self.kite = kite_client
        self.positions = positions

    def sync(self):
        """
        Reconcile runtime positions with Kite.
        """
        sync_positions_from_kite(self.kite, self.positions)

    def enter_position(self, *, token, signal, qty, option):
        """
        Place a live market order via Kite and store strategy-scoped position.
        """

        self.sync()  # 🔒 ALWAYS sync before placing live orders

        tradingsymbol = option["tradingsymbol"]

        order_id = self.kite.place_order(
            variety=self.kite.VARIETY_REGULAR,
            exchange="NFO",
            tradingsymbol=tradingsymbol,
            transaction_type=(
                self.kite.TRANSACTION_TYPE_BUY
                if signal["direction"] == "LONG"
                else self.kite.TRANSACTION_TYPE_SELL
            ),
            quantity=qty,
            product=self.kite.PRODUCT_NRML,
            order_type=self.kite.ORDER_TYPE_MARKET
        )

        # Strategy-scoped position key
        position_key = (signal["strategy"], tradingsymbol)

        self.positions[position_key] = {
            "strategy": signal["strategy"],
            "token": token,
            "index": option["name"],
            "direction": signal["direction"],
            "tradingsymbol": tradingsymbol,
            "instrument_token": option["instrument_token"],
            "strike": option["strike"],
            "expiry": option["expiry"],
            "qty": qty,
            "open": True,
            "kite_order_id": order_id
        }

        logger.info(
            f"LIVE ENTRY | {signal['strategy']} | "
            f"{tradingsymbol} | qty={qty} | order_id={order_id}"
        )

        return order_id
