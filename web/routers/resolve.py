"""Router for browser-based manifest resolution."""

import threading

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

router = APIRouter()


class ResolveRequest(BaseModel):
    url: str
    title: str | None = None
    timeout: int = 60


class BatchResolveRequest(BaseModel):
    urls: list[dict]
    timeout: int = 60


@router.post("/resolve")
def resolve_manifest(req: ResolveRequest):
    from core.resolver.manifest_resolver import ManifestResolverService

    status = ManifestResolverService.get_status()
    if status["running"]:
        return JSONResponse({"error": "resolve already running", "current_url": status["last_url"]}, status_code=409)

    thread = threading.Thread(
        target=ManifestResolverService.resolve,
        args=(req.url,),
        kwargs={"title": req.title, "timeout": req.timeout},
        daemon=True,
    )
    thread.start()
    return {"ok": True, "message": f"Resolving {req.url}"}


@router.get("/resolve/status")
def resolve_status():
    from core.resolver.manifest_resolver import ManifestResolverService
    return ManifestResolverService.get_status()


@router.get("/resolve/selenium-status")
def selenium_status():
    from core.resolver.manifest_resolver import ManifestResolverService
    return {"ready": ManifestResolverService.check_selenium()}


@router.post("/resolve/batch")
def resolve_batch(req: BatchResolveRequest):
    from core.resolver.manifest_resolver import ManifestResolverService

    batch = ManifestResolverService.get_batch_status()
    if batch["running"]:
        return JSONResponse({"error": "batch already running"}, status_code=409)

    thread = threading.Thread(
        target=ManifestResolverService.resolve_batch,
        args=(req.urls,),
        kwargs={"timeout": req.timeout},
        daemon=True,
    )
    thread.start()
    return {"ok": True, "total": len(req.urls)}


@router.get("/resolve/batch/status")
def batch_status():
    from core.resolver.manifest_resolver import ManifestResolverService
    return ManifestResolverService.get_batch_status()


@router.get("/resolve/channels")
def list_resolved_channels():
    """List all manifests in the library (B3: this is now a manifest library,
    not an auto-channel list).

    Each row includes a `channels` list with the names + ids of any Channel
    rows referencing this manifest, plus a `channel_count`. The UI uses these
    to show 'Used in N channels' and to gate the 'Create Channel' action.
    """
    from core.database import get_session
    from core.models.manifest import Manifest, Capture
    from core.models.channel import Channel

    with get_session() as session:
        manifests = (
            session.query(
                Manifest.id,
                Manifest.title,
                Manifest.url,
                Manifest.expires_at,
                Capture.page_url,
            )
            .outerjoin(Capture, Manifest.capture_id == Capture.id)
            .filter(Manifest.active == True)
            .filter(Manifest.tags.contains(["resolved"]))
            .order_by(Manifest.created_at.desc())
            .all()
        )

        # Bulk-load channel references in one query, then group by manifest_id
        manifest_ids = [m.id for m in manifests]
        channels_by_manifest: dict[str, list] = {}
        if manifest_ids:
            ch_rows = (
                session.query(Channel.id, Channel.name, Channel.manifest_id)
                .filter(Channel.manifest_id.in_(manifest_ids))
                .all()
            )
            for cid, cname, mid in ch_rows:
                channels_by_manifest.setdefault(mid, []).append({"id": cid, "name": cname})

    return {
        "results": [
            {
                "url": m.page_url or m.url,
                "title": m.title,
                "status": "done",
                "manifest_id": m.id,
                "manifest_url": m.url,
                "expires_at": m.expires_at.isoformat() if m.expires_at else None,
                "channels": channels_by_manifest.get(m.id, []),
                "channel_count": len(channels_by_manifest.get(m.id, [])),
                "error": None,
            }
            for m in manifests
        ]
    }


@router.delete("/resolve/channels/{manifest_id}")
def delete_resolved_channel(manifest_id: str):
    """Permanently delete a resolved channel (manifest + variants).

    Captures are kept (capture_id set to NULL) so the historical record of
    'we once resolved this page' survives. Variants cascade-delete with the
    manifest. After delete, regenerate the M3U so the channel disappears
    from the export right away.
    """
    from core.database import get_session
    from core.models.manifest import Manifest

    with get_session() as session:
        row = session.query(Manifest).filter_by(id=manifest_id).first()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        title = row.title
        session.delete(row)

    # Refresh M3U so downstream consumers (e.g. manifold) drop it on next ingest
    try:
        from web import shared_state
        shared_state.regenerate_m3u()
    except Exception as e:
        import logging
        logging.warning("[RESOLVER] regenerate_m3u after delete failed: %s", e)

    return {"ok": True, "deleted": manifest_id, "title": title}


@router.post("/resolve/retry/{index}")
def retry_item(index: int):
    from core.resolver.manifest_resolver import ManifestResolverService

    batch = ManifestResolverService.get_batch_status()
    if batch["running"]:
        return JSONResponse({"error": "batch is currently running"}, status_code=409)

    thread = threading.Thread(
        target=ManifestResolverService.retry_batch_item,
        args=(index,),
        daemon=True,
    )
    thread.start()
    return {"ok": True}


@router.post("/resolve/refresh/{manifest_id}")
def refresh_single(manifest_id: str):
    """Manually trigger a re-resolve of a specific manifest from its stored page_url."""
    from core.resolver.manifest_resolver import ManifestResolverService

    status = ManifestResolverService.get_status()
    if status["running"]:
        return JSONResponse({"error": "resolve already running"}, status_code=409)

    thread = threading.Thread(
        target=ManifestResolverService.refresh_manifest,
        args=(manifest_id,),
        daemon=True,
    )
    thread.start()
    return {"ok": True, "message": f"Refreshing {manifest_id}"}
