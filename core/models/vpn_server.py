"""Persistent server-by-server VPN performance log.

Each unique exit IP we observe gets a row. As samples roll in, we accumulate
min/max/sum of RTT, total samples, success count, and connected duration.
Avg RTT and success rate are computed at read time from these aggregates.
"""

import uuid
from datetime import datetime

from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, Index

from core.models.base import Base


class VpnServer(Base):
    __tablename__ = "vpn_servers"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    ip = Column(String, unique=True, nullable=False, index=True)
    city = Column(String)
    country = Column(String)
    hostname = Column(String, nullable=True)
    org = Column(String, nullable=True)

    first_seen_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    last_seen_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    last_sample_at = Column(DateTime(timezone=True), nullable=True)

    total_samples = Column(Integer, nullable=False, default=0)
    successful_samples = Column(Integer, nullable=False, default=0)

    min_rtt_ms = Column(Float, nullable=True)
    max_rtt_ms = Column(Float, nullable=True)
    sum_rtt_ms = Column(Float, nullable=True)

    total_seconds_connected = Column(Integer, nullable=False, default=0)
    is_current = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        Index("ix_vpn_servers_last_seen_desc", last_seen_at.desc()),
        Index("ix_vpn_servers_is_current", is_current),
    )
