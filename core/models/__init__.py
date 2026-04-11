"""Database models for Channelarr.

In Phase B of the resolver-channel refactor, the `channels` table is being
introduced as the unified store for both scheduled (local + YouTube) and
resolved (browser-captured live stream) channels. JSON channels.json is still
the source of truth for scheduled channels during B1; reads switch in B2.
"""

from core.models.base import Base
from core.models.channel import Channel
from core.models.manifest import Capture, Manifest, Variant, HeaderProfile

__all__ = ["Base", "Channel", "Capture", "Manifest", "Variant", "HeaderProfile"]
