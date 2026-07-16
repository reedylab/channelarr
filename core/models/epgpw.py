"""Cached epg.pw guide data.

One row per mapped epg.pw channel id, holding the raw XMLTV document fetched
from epg.pw. Cached (rather than fetched at export time) because the XMLTV
export regenerates often — on every scrape, channel edit, and cleanup pass —
and epg.pw is a free public service we shouldn't hammer.
"""

from datetime import datetime

from sqlalchemy import Column, String, Text, DateTime

from core.models.base import Base


class EpgPwCache(Base):
    __tablename__ = "epgpw_cache"

    # epg.pw channel id (the `channel_id` query param of their XMLTV API)
    epg_pw_id = Column(String, primary_key=True)
    # Raw XMLTV document as returned by epg.pw
    xml = Column(Text, nullable=False)
    # Human-readable epg.pw channel name, for display/debugging
    name = Column(String, nullable=True)
    fetched_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
