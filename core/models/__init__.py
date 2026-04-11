"""Database models — Postgres parallel storage for the resolver.

Existing channelarr channels (local media + YouTube) remain in JSON files.
This package is specifically for resolved channels captured via the browser
sidecar.
"""

from core.models.base import Base
from core.models.manifest import Capture, Manifest, Variant, HeaderProfile

__all__ = ["Base", "Capture", "Manifest", "Variant", "HeaderProfile"]
