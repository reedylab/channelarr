"""Periodic (~60s) rollup of core/diagnostics.py's in-memory stream telemetry.

One row per channel per rollup tick. Exists so diagnostic history survives a
stream restarting/idling out (the in-memory ring buffers don't) and so
source quality can eventually be compared across days, not just within one
sitting.
"""

import uuid
from datetime import datetime

from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, Index

from core.models.base import Base


class StreamDiagnosticSnapshot(Base):
    __tablename__ = "stream_diagnostic_snapshots"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    channel_id = Column(String, index=True, nullable=False)
    ts = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow, index=True)

    source_kind = Column(String, nullable=True)      # "hls" | "relay" | "local" | ... or None
    encoder_mode = Column(String, nullable=True)      # "proxy" | "remux" | "single" | "multi" | "copy" | None

    encode_speed_ratio = Column(Float, nullable=True)     # None if this stream's mode doesn't re-encode
    fetch_latency_ms_avg = Column(Float, nullable=True)
    fetch_latency_ms_max = Column(Float, nullable=True)
    reconnect_count = Column(Integer, nullable=False, default=0)
    reconnect_gap_ms_max = Column(Float, nullable=True)
    error_count = Column(Integer, nullable=False, default=0)
    fallback_active = Column(Boolean, nullable=False, default=False)
    playlist_wait_ms_max = Column(Float, nullable=True)
    quality_badge = Column(String, nullable=False, default="unknown")

    __table_args__ = (
        Index("ix_stream_diag_channel_ts", "channel_id", ts.desc()),
    )
