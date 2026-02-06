"""
db/repository.py

Repository layer for safe, idempotent writes to Postgres.
"""

import logging
from sqlalchemy.exc import IntegrityError

from data.time_utils import now_ist
from db.models import Signal, Trade, Position, DailyPnl

logger = logging.getLogger(__name__)


class SignalRepository:
    def __init__(self, session):
        self.session = session

    def log_signal(
        self,
        *,
        strategy,
        token,
        direction,
        price,
        signal_time=None,
        accepted=False,
        reject_reason=None
    ):
        signal = Signal(
            strategy=strategy,
            token=token,
            direction=direction,
            price=price,
            signal_time=signal_time or now_ist(),
            accepted=accepted,
            reject_reason=reject_reason
        )
        self.session.add(signal)
        self.session.flush()
        return signal


class TradeRepository:
    def __init__(self, session):
        self.session = session

    def upsert_trade_entry(
        self,
        *,
        trade_id,
        strategy,
        token,
        index,
        direction,
        qty,
        entry_price,
        entry_time=None
    ):
        entry_time = entry_time or now_ist()
        trade = Trade(
            trade_id=trade_id,
            strategy=strategy,
            token=token,
            index=index,
            direction=direction,
            qty=qty,
            entry_price=entry_price,
            entry_time=entry_time
        )
        try:
            self.session.add(trade)
            self.session.flush()
            return trade
        except IntegrityError:
            self.session.rollback()
            existing = (
                self.session.query(Trade)
                .filter(Trade.trade_id == trade_id)
                .one_or_none()
            )
            return existing

    def update_trade_exit(
        self,
        *,
        trade_id,
        exit_price,
        exit_time=None,
        exit_reason=None,
        pnl=None
    ):
        trade = (
            self.session.query(Trade)
            .filter(Trade.trade_id == trade_id)
            .one_or_none()
        )
        if not trade:
            return None
        trade.exit_price = exit_price
        trade.exit_time = exit_time or now_ist()
        trade.exit_reason = exit_reason
        trade.pnl = pnl
        return trade


class PositionRepository:
    def __init__(self, session):
        self.session = session

    def upsert_position(
        self,
        *,
        position_id,
        token,
        strategy,
        index,
        direction,
        qty,
        entry_price,
        entry_time=None
    ):
        entry_time = entry_time or now_ist()
        position = (
            self.session.query(Position)
            .filter(Position.position_id == position_id)
            .one_or_none()
        )
        if position:
            return position
        position = Position(
            position_id=position_id,
            token=token,
            strategy=strategy,
            index=index,
            direction=direction,
            qty=qty,
            entry_price=entry_price,
            entry_time=entry_time,
            open=True
        )
        self.session.add(position)
        self.session.flush()
        return position

    def mark_position(
        self,
        *,
        position_id,
        last_price,
        last_time=None,
        pnl=None
    ):
        position = (
            self.session.query(Position)
            .filter(Position.position_id == position_id)
            .one_or_none()
        )
        if not position:
            return None
        position.last_price = last_price
        position.last_time = last_time or now_ist()
        position.pnl = pnl
        return position

    def close_position(
        self,
        *,
        position_id,
        exit_price,
        exit_time=None,
        pnl=None
    ):
        position = (
            self.session.query(Position)
            .filter(Position.position_id == position_id)
            .one_or_none()
        )
        if not position:
            return None
        position.last_price = exit_price
        position.last_time = exit_time or now_ist()
        position.pnl = pnl
        position.open = False
        return position


class DailyPnlRepository:
    def __init__(self, session):
        self.session = session

    def upsert_daily_pnl(
        self,
        *,
        token,
        date,
        pnl,
        strategy=None,
        index=None
    ):
        record = (
            self.session.query(DailyPnl)
            .filter(
                DailyPnl.token == token,
                DailyPnl.date == date
            )
            .one_or_none()
        )
        if record:
            record.pnl = pnl
            record.updated_at = now_ist()
            return record
        record = DailyPnl(
            token=token,
            strategy=strategy,
            index=index,
            date=date,
            pnl=pnl,
            updated_at=now_ist()
        )
        self.session.add(record)
        self.session.flush()
        return record
