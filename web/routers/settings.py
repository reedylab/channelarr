"""Settings endpoints."""

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from web import shared_state
from core.config import get_all_settings, save_settings

router = APIRouter()


@router.get("/settings")
def api_get_settings():
    try:
        values = get_all_settings()
        return {"schema": shared_state.SETTINGS_SCHEMA, "values": values}
    except Exception as e:
        logging.exception("[SETTINGS] Failed to get settings")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/settings")
async def api_save_settings(request: Request):
    data = await request.json()
    valid_keys = set()
    for section in shared_state.SETTINGS_SCHEMA.values():
        valid_keys.update(section["fields"].keys())
    filtered = {k: str(v) for k, v in data.items() if k in valid_keys}
    if not filtered:
        return {"status": "ok", "message": "No changes."}
    save_settings(filtered)
    return {"status": "ok", "updated": list(filtered.keys()), "message": "Settings saved."}
