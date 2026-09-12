"""Rolling reliability history for multi-player-source "player path"
candidates.

Some resolved-channel source families expose the same underlying stream
through several alternate player paths (naming and discovery logic live in
the gitignored native-resolver plugin) that fail independently of one
another. `player_path` is the stable identity to key history on — the
actual manifest URL/token for a given path rotates every resolve.

Backs auto-ranking of primary/fallback candidates so that choice stops
being a static, manually-set one — see core/resolver/player_health.py for
the scoring/promotion logic that reads and writes this table.
"""

import uuid
from datetime import datetime

from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, UniqueConstraint

from core.models.base import Base


class PlayerHealthScore(Base):
    __tablename__ = "player_health_scores"
    __table_args__ = (
        UniqueConstraint("channel_id", "player_path", name="uq_player_health_channel_path"),
    )

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    channel_id = Column(String, nullable=False, index=True)
    player_path = Column(String, nullable=False)

    success_count = Column(Integer, nullable=False, default=0)
    failure_count = Column(Integer, nullable=False, default=0)
    # Streak counters weigh recent behavior more heavily than lifetime
    # average — a path that's failed its last 5 probes should rank below one
    # with a similar lifetime success rate but currently healthy.
    consecutive_successes = Column(Integer, nullable=False, default=0)
    consecutive_failures = Column(Integer, nullable=False, default=0)

    last_probed_at = Column(DateTime(timezone=True), nullable=True)
    last_ok = Column(Boolean, nullable=True)
    last_latency_ms = Column(Float, nullable=True)
    last_error = Column(String, nullable=True)

    updated_at = Column(DateTime(timezone=True), nullable=False,
                        default=datetime.utcnow, onupdate=datetime.utcnow)
