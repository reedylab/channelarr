"""HDHomeRun emulation — makes Channelarr discoverable by Plex as a tuner."""

import os
import uuid
import logging

from fastapi import APIRouter, Request
from fastapi.responses import Response

from web import shared_state
from core.config import get_setting

router = APIRouter()

DEVICE_ID = "CHNLARR1"
DEVICE_UUID = str(uuid.uuid5(uuid.NAMESPACE_DNS, "channelarr.hdhr"))


def _base_url(request: Request) -> str:
    """Use BASE_URL from settings, fall back to request origin."""
    configured = get_setting("BASE_URL", "").rstrip("/")
    if configured and configured != "http://localhost:5045":
        return configured
    return str(request.base_url).rstrip("/")


@router.get("/discover.json")
def discover(request: Request):
    base = _base_url(request)
    return {
        "FriendlyName": "Channelarr",
        "Manufacturer": "Channelarr",
        "ModelNumber": "HDTC-2US",
        "FirmwareName": "hdhomerun_atsc",
        "TunerCount": 2,
        "FirmwareVersion": "20250401",
        "DeviceID": DEVICE_ID,
        "DeviceAuth": "channelarr",
        "BaseURL": base,
        "LineupURL": f"{base}/lineup.json",
    }


@router.get("/lineup_status.json")
def lineup_status():
    return {
        "ScanInProgress": 0,
        "ScanPossible": 1,
        "Source": "Cable",
        "SourceList": ["Cable"],
    }


@router.get("/lineup.json")
def lineup(request: Request):
    base = _base_url(request)
    channels = shared_state.channel_mgr.list_channels()
    result = []
    for i, ch in enumerate(channels, start=1):
        logo_path = os.path.join(shared_state.LOGO_DIR, f"{ch['id']}.png")
        entry = {
            "GuideNumber": str(i),
            "GuideName": ch.get("name", f"Channel {i}"),
            "URL": f"{base}/live/{ch['id']}/stream.m3u8",
        }
        if os.path.isfile(logo_path):
            entry["ImageURL"] = f"{base}/api/logo/{ch['id']}"
        result.append(entry)
    logging.info("[HDHR] Lineup requested — %d channels", len(result))
    return result


@router.get("/device.xml")
def device_xml(request: Request):
    base = _base_url(request)
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <specVersion>
    <major>1</major>
    <minor>0</minor>
  </specVersion>
  <URLBase>{base}</URLBase>
  <device>
    <deviceType>urn:schemas-upnp-org:device:MediaServer:1</deviceType>
    <friendlyName>Channelarr</friendlyName>
    <manufacturer>Channelarr</manufacturer>
    <modelName>HDTC-2US</modelName>
    <modelNumber>HDTC-2US</modelNumber>
    <serialNumber>{DEVICE_ID}</serialNumber>
    <UDN>uuid:{DEVICE_UUID}</UDN>
  </device>
</root>"""
    return Response(content=xml, media_type="application/xml")
