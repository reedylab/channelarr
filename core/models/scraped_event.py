"""ScrapedEvent model — pending-event queue for scraper→resolver JIT.

Scrapers discover events cheaply (HTTP only) and upsert rows here. The
event resolver later picks rows whose event_start is near now, hands them
to the selenium-backed batch resolver, and transitions status.
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    Column, String, Integer, DateTime, Text, ForeignKey, Index, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from core.models.base import Base


class ScrapedEvent(Base):
    __tablename__ = "scraped_events"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    scraper_name = Column(String, nullable=False, index=True)
    url = Column(Text, nullable=False)
    url_hash = Column(String(32), nullable=False, index=True)
    title = Column(String)
    tags = Column(JSONB, default=list, nullable=False)
    logo_urls = Column(JSONB, default=list, nullable=False)
    event_start = Column(DateTime(timezone=True), nullable=True, index=True)
    event_end = Column(DateTime(timezone=True), nullable=True)
    status = Column(String, nullable=False, default="pending")
    channel_id = Column(
        String,
        ForeignKey("channels.id", ondelete="SET NULL"),
        nullable=True,
    )
    attempt_count = Column(Integer, default=0, nullable=False)
    last_attempt_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)
    discovered_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("scraper_name", "url_hash", name="uq_scraped_events_scraper_urlhash"),
        Index("ix_scraped_events_status_event_start", "status", "event_start"),
    )
