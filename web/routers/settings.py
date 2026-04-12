"""Settings endpoints."""

import logging
import os
import re

from fastapi import APIRouter, Request, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse

from web import shared_state
from core.config import get_all_settings, save_settings, get_setting

router = APIRouter()

BRANDING_DIR = None

def _branding_dir():
    global BRANDING_DIR
    if BRANDING_DIR is None:
        base = get_setting("DATA_PATH", "/app/data")
        BRANDING_DIR = os.path.join(base, "branding")
    os.makedirs(BRANDING_DIR, exist_ok=True)
    return BRANDING_DIR


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


@router.get("/branding")
def api_list_branding():
    bd = _branding_dir()
    logos = []
    for f in sorted(os.listdir(bd)):
        if f.lower().endswith((".png", ".jpg", ".jpeg")):
            logos.append({"filename": f, "url": f"/api/branding/{f}"})
    return logos


@router.get("/branding/{filename}")
def api_get_branding(filename: str):
    bd = _branding_dir()
    safe = os.path.basename(filename)
    path = os.path.join(bd, safe)
    if not os.path.isfile(path):
        return JSONResponse({"error": "Not found"}, status_code=404)
    return FileResponse(path, media_type="image/png")


@router.post("/branding")
async def api_upload_branding(file: UploadFile = File(...)):
    data = await file.read()
    if len(data) < 8:
        return JSONResponse({"error": "File too small"}, status_code=400)
    is_png = data[:4] == b"\x89PNG"
    is_jpeg = data[:2] == b"\xff\xd8"
    if not (is_png or is_jpeg):
        return JSONResponse({"error": "Only PNG or JPEG allowed"}, status_code=400)

    name = os.path.splitext(file.filename or "logo")[0]
    name = re.sub(r"[^\w\-]", "_", name).strip("_")[:50] or "logo"
    ext = ".png" if is_png else ".jpg"
    filename = f"{name}{ext}"

    bd = _branding_dir()
    path = os.path.join(bd, filename)
    counter = 1
    while os.path.exists(path):
        path = os.path.join(bd, f"{name}_{counter}{ext}")
        filename = f"{name}_{counter}{ext}"
        counter += 1

    with open(path, "wb") as f:
        f.write(data)
    logging.info("[BRANDING] Uploaded %s", filename)
    return {"filename": filename, "url": f"/api/branding/{filename}"}


@router.delete("/branding/{filename}")
def api_delete_branding(filename: str):
    bd = _branding_dir()
    safe = os.path.basename(filename)
    path = os.path.join(bd, safe)
    if not os.path.isfile(path):
        return JSONResponse({"error": "Not found"}, status_code=404)
    os.remove(path)
    logging.info("[BRANDING] Deleted %s", safe)
    return {"status": "deleted"}
