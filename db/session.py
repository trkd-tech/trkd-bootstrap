"""
db/session.py

SQLAlchemy engine/session helpers for Cloud Run-safe access.
"""

import os
import logging
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

_engine = None
_session_factory = None


def get_database_url():
    return os.getenv("DATABASE_URL")


def get_engine():
    global _engine
    if _engine is not None:
        return _engine

    url = get_database_url()
    if not url:
        logger.error("DATABASE_URL is not set; DB writes are disabled.")
        return None

    _engine = create_engine(
        url,
        pool_pre_ping=True,
        future=True
    )
    return _engine


def get_session_factory():
    global _session_factory
    if _session_factory is not None:
        return _session_factory

    engine = get_engine()
    if engine is None:
        return None

    _session_factory = sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        future=True
    )
    return _session_factory


@contextmanager
def db_session():
    """
    Session-based DB access with safe commit/rollback.
    Yields None if DB is not configured, so callers can skip writes.
    """
    session_factory = get_session_factory()
    if session_factory is None:
        yield None
        return

    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        logger.exception("DB session failed")
        raise
    finally:
        session.close()
