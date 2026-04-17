"""System stats, log tail, and VPN management endpoints."""

from pathlib import Path

import requests as http_requests
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from core.config import get_setting
from web import shared_state

router = APIRouter()


@router.get("/system/stats")
def api_system_stats():
    return shared_state.get_stats_snapshot()


@router.get("/tasks/status")
def api_tasks_status():
    """Return all background tasks and scheduler jobs."""
    return {"tasks": shared_state.get_tasks()}


# ── VPN ──────────────────────────────────────────────────────────────────

@router.get("/vpn/status")
def vpn_status():
    url = get_setting("GLUETUN_CONTROL_URL", "")
    if not url:
        return {"enabled": False, "status": "not configured"}
    try:
        user = get_setting("GLUETUN_CONTROL_USER", "")
        password = get_setting("GLUETUN_CONTROL_PASS", "")
        auth = (user, password) if user else None

        try:
            r = http_requests.get(f"{url}/v1/vpn/status", auth=auth, timeout=3)
            r.raise_for_status()
            status_data = r.json()
        except Exception:
            r = http_requests.get(f"{url}/v1/openvpn/status", auth=auth, timeout=3)
            status_data = r.json()
        vpn_st = status_data.get("status", "unknown")

        ip = ""
        country = ""
        city = ""
        try:
            ip_resp = http_requests.get(f"{url}/v1/publicip/ip", auth=auth, timeout=3)
            ip_data = ip_resp.json()
            ip = ip_data.get("public_ip", "")
            country = ip_data.get("country", "")
            city = ip_data.get("city", ip_data.get("region", ""))
        except Exception:
            pass

        if not ip and vpn_st == "running":
            try:
                ext = http_requests.get("https://api.ipify.org?format=json", timeout=5)
                ip = ext.json().get("ip", "")
            except Exception:
                pass
            if ip and not city:
                try:
                    geo = http_requests.get(f"http://ip-api.com/json/{ip}?fields=city,country", timeout=3)
                    geo_data = geo.json()
                    city = geo_data.get("city", "")
                    country = geo_data.get("country", "")
                except Exception:
                    pass

        return {
            "enabled": True,
            "status": vpn_st,
            "ip": ip,
            "country": country,
            "city": city,
        }
    except Exception as e:
        return {"enabled": True, "status": "unreachable", "error": str(e)}


@router.get("/vpn/history")
def vpn_history(minutes: int = Query(default=60, ge=1, le=1440)):
    from core.vpn_monitor import get_history, get_summary
    return {
        "summary": get_summary(),
        "samples": get_history(minutes),
    }


@router.get("/vpn/servers")
def vpn_servers(
    sort: str = Query(default="avg_rtt"),
    order: str = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
):
    from core.vpn_monitor import list_servers
    return {"servers": list_servers(sort=sort, order=order, limit=limit)}


@router.post("/vpn/rotate")
def vpn_rotate():
    from core.vpn_monitor import rotate_vpn
    result = rotate_vpn(reason="manual")
    if not result.get("ok"):
        return JSONResponse(result, status_code=502)
    return result


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
