"""Channel model — unified storage for scheduled and resolved channels.

Phase B1 of the resolver-channel refactor: introduces a Postgres `channels`
table that mirrors the existing JSON channel store. JSON is still the source
of truth in B1; this table is dual-written to but not yet read from.

Two channel types:
  - "scheduled": has items + materialized_schedule, served via /live/{id}/stream.m3u8
                 by the FFmpeg streamer. Items can mix local media and YouTube.
  - "resolved":  references a Manifest via manifest_id, served via
                 /live-resolved/{manifest_id}.m3u8 as a proxy to upstream HLS.
                 No items, no schedule, no bumps (until step 6).
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    Column, String, Boolean, DateTime, Float, ForeignKey, Index, Enum,
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB

from core.models.base import Base


class Channel(Base):
    __tablename__ = "channels"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    type = Column(
        Enum("scheduled", "resolved", name="channel_type"),
        nullable=False,
        default="scheduled",
    )
    logo_filename = Column(String, nullable=True)

    # Scheduled-channel fields (local + YouTube items mixed allowed)
    items = Column(JSONB, default=list, nullable=False)
    bump_config = Column(JSONB, default=dict, nullable=False)
    shuffle_config = Column(JSONB, default=dict, nullable=False)
    loop = Column(Boolean, default=True, nullable=False)
    schedule_epoch = Column(DateTime(timezone=True), nullable=True)
    schedule_cycle_duration = Column(Float, default=0, nullable=False)
    materialized_schedule = Column(JSONB, default=list, nullable=False)

    # Resolved-channel field — FK to the manifest powering the live stream.
    # Many-to-one: multiple channels can reference the same manifest.
    manifest_id = Column(
        String,
        ForeignKey("manifests.id", ondelete="CASCADE"),
        nullable=True,
    )

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    manifest = relationship("Manifest", backref="channels")

    __table_args__ = (
        Index("ix_channels_type", "type"),
        Index("ix_channels_manifest_id", "manifest_id"),
    )
