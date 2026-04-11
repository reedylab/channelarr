"""Resolver-specific models: Capture, Manifest, Variant, HeaderProfile.

Trimmed down from manifold's model — keeps only fields the resolver actually
writes. Manifold-specific aggregation fields (m3u_source_id, tvg_*, stream_mode,
channel_number, etc.) are not present here.
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    Column, String, Integer, Float, Boolean, DateTime, Text, ForeignKey, Enum,
    UniqueConstraint, Index,
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB

from core.models.base import Base


class HeaderProfile(Base):
    __tablename__ = "header_profiles"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, unique=True, nullable=False)
    headers = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    manifests = relationship("Manifest", back_populates="header_profile")


class Capture(Base):
    __tablename__ = "captures"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    page_url = Column(Text, nullable=False)
    user_agent = Column(Text)
    context = Column(JSONB, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    manifests = relationship("Manifest", back_populates="capture")


class Manifest(Base):
    __tablename__ = "manifests"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    capture_id = Column(String, ForeignKey("captures.id", ondelete="SET NULL"))
    header_profile_id = Column(String, ForeignKey("header_profiles.id", ondelete="SET NULL"))
    # channelarr-style identifier (ch-res-xxxxxxxx) for unified handling with JSON channels
    channelarr_channel_id = Column(String, unique=True, index=True, nullable=True)
    url = Column(Text, nullable=False)
    url_hash = Column(String(32), nullable=False)
    source_domain = Column(String, index=True)
    mime = Column(String)
    kind = Column(Enum("master", "media", name="manifest_kind"), nullable=False)
    headers = Column(JSONB, default=dict)
    requires_headers = Column(Boolean, default=False, nullable=False)
    body = Column(Text)
    sha256 = Column(String(64))
    drm_method = Column(String)
    is_drm = Column(Boolean, default=False, nullable=False)
    title = Column(String)
    tags = Column(JSONB, default=list)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    expires_at = Column(DateTime(timezone=True), nullable=True)
    last_refreshed_at = Column(DateTime(timezone=True), nullable=True)
    last_accessed_at = Column(DateTime(timezone=True), nullable=True)

    capture = relationship("Capture", back_populates="manifests")
    header_profile = relationship("HeaderProfile", back_populates="manifests")
    variants = relationship("Variant", back_populates="manifest", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("url_hash", "sha256", name="uq_manifest_urlhash_bodyhash"),
        Index(
            "uq_manifests_title_active",
            "title",
            unique=True,
            postgresql_where=(active == True),
        ),
        Index("ix_manifests_created_desc", created_at.desc()),
    )


class Variant(Base):
    __tablename__ = "variants"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    manifest_id = Column(String, ForeignKey("manifests.id", ondelete="CASCADE"), index=True)
    uri = Column(Text, nullable=False)
    abs_url = Column(Text, nullable=False)
    bandwidth = Column(Integer)
    resolution = Column(String)
    frame_rate = Column(Float)
    codecs = Column(String)
    audio_group = Column(String)
    width = Column(Integer)
    height = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    manifest = relationship("Manifest", back_populates="variants")

    __table_args__ = (
        Index("ix_variants_bw_desc", bandwidth.desc()),
    )
