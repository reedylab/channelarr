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
