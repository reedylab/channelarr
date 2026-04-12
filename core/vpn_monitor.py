"""VPN latency sampling and gluetun control.

Channelarr runs inside its own gluetun network namespace, so a ping from this
container goes through the WireGuard tunnel and gives us the true round-trip
time of the VPN. Sampled every 60 seconds and surfaced in the System Stats
tab as a chart alongside CPU/RAM/Disk.

Auto-rotate cycles the WireGuard tunnel via gluetun's control API
(PUT /v1/vpn/status) WITHOUT recreating the gluetun container — so attached
services keep their network namespace.
"""

import logging
import socket
import threading
import time
from collections import deque
from datetime import datetime, timezone, timedelta

import requests as http_requests

logger = logging.getLogger(__name__)

# 1440 samples = 24h of history at one sample per minute
_samples = deque(maxlen=1440)
_lock = threading.Lock()
_last_rotate_at = None


def _get_auth_and_url():
    """Resolve gluetun control config from env vars via core.config."""
    from core.config import get_setting
    url = get_setting("GLUETUN_CONTROL_URL", "")
    if not url:
        return None, None
    user = get_setting("GLUETUN_CONTROL_USER", "")
    password = get_setting("GLUETUN_CONTROL_PASS", "")
    auth = (user, password) if user else None
    return url, auth


def _ping_rtt(target: str = "1.1.1.1", port: int = 443, timeout: int = 2) -> float | None:
    """Measure RTT via TCP connect to target:port. Return ms or None on failure.

    Uses a TCP handshake instead of ICMP ping so we don't need the ping
    binary or CAP_NET_RAW in the container.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        start = time.monotonic()
        sock.connect((target, port))
        rtt = (time.monotonic() - start) * 1000
        sock.close()
        return round(rtt, 2)
    except Exception as e:
        logger.debug("[VPN-MONITOR] tcp ping failed: %s", e)
        return None


def _fetch_exit_info() -> dict:
    """Return exit info from gluetun's /v1/publicip/ip endpoint.

    Returns {ip, city, country, hostname, org}. Empty strings for missing
    fields. Returns {} on error.
    """
    base, auth = _get_auth_and_url()
    if not base:
        return {}
    try:
        r = http_requests.get(f"{base}/v1/publicip/ip", auth=auth, timeout=3)
        d = r.json()
        return {
            "ip": d.get("public_ip", "") or "",
            "city": (d.get("city") or d.get("region") or ""),
            "country": d.get("country", "") or "",
            "hostname": d.get("hostname", "") or "",
            "org": d.get("organization", "") or "",
        }
    except Exception:
        return {}


def sample_latency():
    """Take one latency sample and append to the rolling history.

    Also upserts the per-server aggregate row in the vpn_servers table so
    we can compute long-term performance stats per exit endpoint.
    """
    rtt = _ping_rtt()
    info = _fetch_exit_info()
    ip = info.get("ip", "")
    city = info.get("city", "")
    now = datetime.now(timezone.utc)

    sample = {
        "ts": now.isoformat(),
        "rtt_ms": rtt,
        "ip": ip,
        "city": city,
    }
    with _lock:
        _samples.append(sample)

    if ip:
        try:
            _upsert_server_row(now, info, rtt)
        except Exception as e:
            logger.warning("[VPN-MONITOR] vpn_servers upsert failed: %s", e)


def _upsert_server_row(now, info: dict, rtt: float | None):
    """Insert or update the vpn_servers row for the current exit IP."""
    from core.database import get_session
    from core.models.vpn_server import VpnServer

    ip = info["ip"]
    with get_session() as session:
        row = session.query(VpnServer).filter_by(ip=ip).first()
        if row is None:
            session.query(VpnServer).filter(VpnServer.is_current == True).update(
                {"is_current": False}
            )
            row = VpnServer(
                ip=ip,
                city=info.get("city") or "",
                country=info.get("country") or "",
                hostname=info.get("hostname") or "",
                org=info.get("org") or "",
                first_seen_at=now,
                last_seen_at=now,
                last_sample_at=now,
                total_samples=1,
                successful_samples=1 if rtt is not None else 0,
                min_rtt_ms=rtt,
                max_rtt_ms=rtt,
                sum_rtt_ms=rtt,
                total_seconds_connected=0,
                is_current=True,
            )
            session.add(row)
            return

        # Refresh enrichment fields if they were missing originally
        if not row.hostname and info.get("hostname"):
            row.hostname = info["hostname"]
        if not row.org and info.get("org"):
            row.org = info["org"]
        if not row.country and info.get("country"):
            row.country = info["country"]

        # Swap current flag if a different server was marked
        if not row.is_current:
            session.query(VpnServer).filter(VpnServer.is_current == True).update(
                {"is_current": False}
            )
            row.is_current = True

        row.last_seen_at = now
        row.total_samples += 1
        if rtt is not None:
            row.successful_samples += 1
            row.sum_rtt_ms = (row.sum_rtt_ms or 0) + rtt
            row.min_rtt_ms = rtt if row.min_rtt_ms is None else min(row.min_rtt_ms, rtt)
            row.max_rtt_ms = rtt if row.max_rtt_ms is None else max(row.max_rtt_ms, rtt)

        # Time delta since last sample on THIS server (avoids inflating across gaps)
        if row.last_sample_at is not None:
            delta = (now - row.last_sample_at).total_seconds()
            if 0 < delta < 300:
                row.total_seconds_connected += int(delta)
        row.last_sample_at = now


def get_history(minutes: int = 60) -> list[dict]:
    """Return all samples within the last N minutes."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    with _lock:
        out = []
        for s in _samples:
            try:
                if datetime.fromisoformat(s["ts"]) >= cutoff:
                    out.append(s)
            except Exception:
                continue
        return out


def get_summary() -> dict:
    """Compute current/min/avg/max RTT plus current exit info.

    The `mode` field tells the UI whether this instance is running behind
    gluetun ("vpn") or on a direct/local network ("local"). When local, the
    rotate button and server history table should be hidden and the card title
    relabeled to "Network Latency".
    """
    from core.config import get_setting
    mode = "vpn" if get_setting("GLUETUN_CONTROL_URL", "") else "local"

    with _lock:
        recent = list(_samples)[-60:]
    if not recent:
        return {
            "mode": mode,
            "current_rtt_ms": None,
            "min_rtt_ms": None,
            "avg_rtt_ms": None,
            "max_rtt_ms": None,
            "current_ip": "",
            "current_city": "",
            "sample_count": 0,
            "last_rotate_at": _last_rotate_at.isoformat() if _last_rotate_at else None,
        }
    rtts = [s["rtt_ms"] for s in recent if s["rtt_ms"] is not None]
    latest = recent[-1]
    summary = {
        "mode": mode,
        "current_rtt_ms": latest["rtt_ms"],
        "current_ip": latest["ip"],
        "current_city": latest["city"],
        "sample_count": len(recent),
        "last_rotate_at": _last_rotate_at.isoformat() if _last_rotate_at else None,
    }
    if rtts:
        summary["min_rtt_ms"] = round(min(rtts), 1)
        summary["max_rtt_ms"] = round(max(rtts), 1)
        summary["avg_rtt_ms"] = round(sum(rtts) / len(rtts), 1)
    else:
        summary["min_rtt_ms"] = None
        summary["max_rtt_ms"] = None
        summary["avg_rtt_ms"] = None
    return summary


def rotate_vpn(reason: str = "manual") -> dict:
    """Cycle the WireGuard tunnel via gluetun control API.

    Returns {"ok": bool, "from": {ip, city}, "to": {ip, city}, "error"?: str}.
    """
    global _last_rotate_at
    base, auth = _get_auth_and_url()
    if not base:
        return {"ok": False, "error": "GLUETUN_CONTROL_URL not configured"}

    before = _fetch_exit_info()
    old_ip = before.get("ip", "")
    old_city = before.get("city", "")
    logger.info("[VPN-MONITOR] Rotating VPN (%s) — current: %s in %s", reason, old_ip, old_city)

    try:
        http_requests.put(
            f"{base}/v1/vpn/status",
            json={"status": "stopped"},
            auth=auth,
            timeout=5,
        ).raise_for_status()
        time.sleep(2)
        http_requests.put(
            f"{base}/v1/vpn/status",
            json={"status": "running"},
            auth=auth,
            timeout=5,
        ).raise_for_status()

        # Poll for new IP, up to 30s
        new_ip, new_city = "", ""
        for _ in range(30):
            time.sleep(1)
            after = _fetch_exit_info()
            new_ip = after.get("ip", "")
            new_city = after.get("city", "")
            if new_ip and new_ip != old_ip:
                break

        _last_rotate_at = datetime.now(timezone.utc)
        logger.info(
            "[VPN-MONITOR] Rotated: %s (%s) → %s (%s)",
            old_ip, old_city, new_ip, new_city,
        )

        # Take an immediate sample so the chart updates right away
        try:
            sample_latency()
        except Exception:
            pass

        return {
            "ok": True,
            "from": {"ip": old_ip, "city": old_city},
            "to": {"ip": new_ip, "city": new_city},
        }
    except Exception as e:
        logger.exception("[VPN-MONITOR] Rotate failed")
        return {"ok": False, "error": str(e)}


def list_servers(sort: str = "avg_rtt", order: str = None, limit: int = 50) -> list[dict]:
    """Return all known VPN servers with computed avg + success rate."""
    from core.database import get_session
    from core.models.vpn_server import VpnServer

    out = []
    with get_session() as session:
        rows = session.query(VpnServer).all()
        for r in rows:
            avg = (r.sum_rtt_ms / r.successful_samples) if (r.successful_samples and r.sum_rtt_ms) else None
            success_rate = (r.successful_samples / r.total_samples) if r.total_samples else 0.0
            out.append({
                "ip": r.ip,
                "city": r.city or "",
                "country": r.country or "",
                "hostname": r.hostname or "",
                "org": r.org or "",
                "is_current": bool(r.is_current),
                "first_seen_at": r.first_seen_at.isoformat() if r.first_seen_at else None,
                "last_seen_at": r.last_seen_at.isoformat() if r.last_seen_at else None,
                "total_samples": r.total_samples,
                "successful_samples": r.successful_samples,
                "success_rate": round(success_rate, 4),
                "min_rtt_ms": round(r.min_rtt_ms, 1) if r.min_rtt_ms is not None else None,
                "max_rtt_ms": round(r.max_rtt_ms, 1) if r.max_rtt_ms is not None else None,
                "avg_rtt_ms": round(avg, 1) if avg is not None else None,
                "total_seconds_connected": r.total_seconds_connected,
            })

    if sort == "avg_rtt":
        out.sort(key=lambda x: (x["avg_rtt_ms"] is None, x["avg_rtt_ms"] or 0),
                 reverse=(order == "desc"))
    elif sort == "last_seen":
        out.sort(key=lambda x: x["last_seen_at"] or "",
                 reverse=(order != "asc"))
    elif sort == "total_samples":
        out.sort(key=lambda x: x["total_samples"], reverse=(order != "asc"))
    elif sort == "success_rate":
        out.sort(key=lambda x: x["success_rate"], reverse=(order != "asc"))
    elif sort == "first_seen":
        out.sort(key=lambda x: x["first_seen_at"] or "",
                 reverse=(order == "desc"))
    elif sort == "connected":
        out.sort(key=lambda x: x["total_seconds_connected"], reverse=(order != "asc"))

    return out[:limit]


def maybe_auto_rotate():
    """Called every 60s. Rotates if interval > 0 AND enough time has passed."""
    from core.config import get_setting
    try:
        minutes = int(get_setting("vpn_auto_rotate_minutes", "0") or "0")
    except (ValueError, TypeError):
        minutes = 0
    if minutes <= 0:
        return
    global _last_rotate_at
    if _last_rotate_at and (datetime.now(timezone.utc) - _last_rotate_at).total_seconds() < minutes * 60:
        return
    rotate_vpn(reason="auto")
