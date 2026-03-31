"""System stats and log tail endpoints."""

from pathlib import Path

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from web import shared_state

router = APIRouter()


@router.get("/system/stats")
def api_system_stats():
    return shared_state.get_stats_snapshot()


@router.get("/logs/tail")
def api_logs_tail(pos: int = Query(default=0), inode: str = Query(default=None)):
    p = Path(shared_state.log_path)

    if not p.exists():
        return {"text": "", "pos": 0, "inode": None, "reset": True}

    st = p.stat()
    inode_token = f"{st.st_dev}:{st.st_ino}"

    reset = False
    if inode and inode != inode_token:
        reset = True
        pos = 0
    elif pos > st.st_size:
        reset = True
        pos = 0

    with open(p, "rb") as f:
        f.seek(pos)
        data = f.read()
        new_pos = pos + len(data)

    text = data.decode("utf-8", errors="replace").replace("\r\n", "\n")
    return {"text": text, "pos": new_pos, "inode": inode_token, "reset": reset}
