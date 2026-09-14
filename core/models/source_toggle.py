"""Manual on/off state for plugin-declared Sources (see core/source_registry.py).

One row per source_id (the stable id a scrapers/ plugin declares in its own
SOURCE_INFO -- never a hardcoded name here). This table holds ONLY the
human's own manual call -- automated down-detection never writes here, it's
computed live instead (see source_registry.is_domain_enabled's docstring for
why). Missing row == manually enabled (the default, opt-out not opt-in).
"""

import uuid
from datetime import datetime

from sqlalchemy import Column, String, Boolean, DateTime

from core.models.base import Base


class SourceToggle(Base):
    __tablename__ = "source_toggles"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    source_id = Column(String, nullable=False, unique=True, index=True)
    manual_enabled = Column(Boolean, nullable=False, default=True)
    updated_at = Column(DateTime(timezone=True), nullable=False,
                        default=datetime.utcnow, onupdate=datetime.utcnow)
