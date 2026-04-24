"""SQLAlchemy engine and session management for the resolver parallel storage.

Existing channelarr channels live in JSON files (see core/channels.py). This
module adds a parallel Postgres layer specifically for resolved channels
(captured via the browser sidecar). A future migration will unify the two.
"""

import logging
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

_engine = None
_SessionFactory = None


def _build_url() -> str:
    from core.config import get_setting
    host = get_setting("PG_HOST", "192.168.20.15")
    port = get_setting("PG_PORT", "5432")
    user = get_setting("PG_USER", "channelarr")
    password = get_setting("PG_PASS", "")
    db = get_setting("PG_DB", "channelarr")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def init_engine():
    """Create the global engine. Safe to call multiple times."""
    global _engine, _SessionFactory
    if _engine is not None:
        return _engine
    url = _build_url()
    _engine = create_engine(url, pool_recycle=3600, pool_pre_ping=True)
    _SessionFactory = sessionmaker(bind=_engine)
    from core.config import get_setting
    logger.info("[DB] Postgres engine initialized: %s:%s/%s",
                get_setting("PG_HOST"), get_setting("PG_PORT"), get_setting("PG_DB"))
    return _engine


def get_engine():
    if _engine is None:
        init_engine()
    return _engine


@contextmanager
def get_session():
    """Yield a SQLAlchemy session with auto commit/rollback."""
    if _SessionFactory is None:
        init_engine()
    session = _SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
