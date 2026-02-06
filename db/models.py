"""
db/models.py

SQLAlchemy ORM models for trading persistence.
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Boolean,
    Index,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import declarative_base

from data.time_utils import now_ist

Base = declarative_base()


class Signal(Base):
    __tablename__ = "signals"
    __table_args__ = (
        Index("ix_signals_token_time", "token", "signal_time"),
    )

    id = Column(Integer, primary_key=True)
    strategy = Column(String, nullable=False)
    token = Column(Integer, nullable=False)
    direction = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    signal_time = Column(DateTime(timezone=True), nullable=False, default=now_ist)
    accepted = Column(Boolean, nullable=False, default=False)
    reject_reason = Column(String, nullable=True)


class Trade(Base):
    __tablename__ = "trades"
    __table_args__ = (
        UniqueConstraint("trade_id", name="uq_trades_trade_id"),
        Index("ix_trades_token", "token"),
        Index("ix_trades_strategy_time", "strategy", "entry_time"),
    )

    id = Column(Integer, primary_key=True)
    trade_id = Column(String, nullable=False)
    strategy = Column(String, nullable=False)
    token = Column(Integer, nullable=False)
    index = Column(String, nullable=False)
    direction = Column(String, nullable=False)
    qty = Column(Integer, nullable=False)
    entry_price = Column(Float, nullable=False)
    entry_time = Column(DateTime(timezone=True), nullable=False, default=now_ist)
    exit_price = Column(Float, nullable=True)
    exit_time = Column(DateTime(timezone=True), nullable=True)
    exit_reason = Column(String, nullable=True)
    pnl = Column(Float, nullable=True)


class Position(Base):
    __tablename__ = "positions"
    __table_args__ = (
        UniqueConstraint("position_id", name="uq_positions_position_id"),
        Index(
            "uq_positions_token_open_true",
            "token",
            unique=True,
            postgresql_where=text("open")
        ),
        Index("ix_positions_token_open", "token", "open"),
        Index("ix_positions_strategy_open", "strategy", "open"),
    )

    id = Column(Integer, primary_key=True)
    position_id = Column(String, nullable=False)
    token = Column(Integer, nullable=False)
    strategy = Column(String, nullable=False)
    index = Column(String, nullable=False)
    direction = Column(String, nullable=False)
    qty = Column(Integer, nullable=False)
    entry_price = Column(Float, nullable=False)
    entry_time = Column(DateTime(timezone=True), nullable=False, default=now_ist)
    open = Column(Boolean, nullable=False, default=True)
    last_price = Column(Float, nullable=True)
    last_time = Column(DateTime(timezone=True), nullable=True)
    pnl = Column(Float, nullable=True)


class DailyPnl(Base):
    __tablename__ = "daily_pnl"
    __table_args__ = (
        UniqueConstraint("token", "date", name="uq_daily_pnl_key"),
        Index("ix_daily_pnl_token_date", "token", "date"),
    )

    id = Column(Integer, primary_key=True)
    token = Column(Integer, nullable=False)
    strategy = Column(String, nullable=True)
    index = Column(String, nullable=True)
    date = Column(DateTime(timezone=True), nullable=False)
    pnl = Column(Float, nullable=False, default=0.0)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=now_ist)
