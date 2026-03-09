"""Generate XMLTV EPG for Channelarr channels."""

import logging
import os
from datetime import datetime, timedelta, timezone
from xml.etree.ElementTree import Element, SubElement, ElementTree, indent

BLOCK_HOURS = 4
TOTAL_HOURS = 48


def generate_channelarr_xmltv(channels: list, output_path: str, base_url: str):
    """Write channelarr.xml with channel definitions and rolling programme blocks."""
    tv = Element("tv", attrib={
        "generator-info-name": "Channelarr",
        "generator-info-url": base_url,
    })

    now = datetime.now(timezone.utc)

    for ch in channels:
        cid = ch["id"]
        name = ch["name"]

        # <channel>
        chan_el = SubElement(tv, "channel", id=cid)
        dn = SubElement(chan_el, "display-name", lang="en")
        dn.text = name
        logo_url = f"{base_url}/api/logo/{cid}"
        SubElement(chan_el, "icon", src=logo_url)

    for ch in channels:
        cid = ch["id"]
        name = ch["name"]
        items = ch.get("items", [])

        # Build description from content items
        desc_parts = []
        for item in items:
            title = item.get("title") or os.path.basename(item.get("path", ""))
            if title:
                desc_parts.append(title)
        desc_text = ", ".join(desc_parts) if desc_parts else "24/7 Channel"

        logo_url = f"{base_url}/api/logo/{cid}"

        # Generate rolling programme blocks
        n_blocks = TOTAL_HOURS // BLOCK_HOURS
        for b in range(n_blocks):
            start = now + timedelta(hours=b * BLOCK_HOURS)
            stop = start + timedelta(hours=BLOCK_HOURS)

            prog = SubElement(tv, "programme", attrib={
                "start": _xmltv_ts(start),
                "stop": _xmltv_ts(stop),
                "channel": cid,
            })
            title_el = SubElement(prog, "title", lang="en")
            title_el.text = name
            desc_el = SubElement(prog, "desc", lang="en")
            desc_el.text = desc_text
            cat_el = SubElement(prog, "category", lang="en")
            cat_el.text = "Channelarr"
            SubElement(prog, "icon", src=logo_url)

    indent(tv, space="  ")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    tree = ElementTree(tv)
    with open(output_path, "wb") as f:
        f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write(b'<!DOCTYPE tv SYSTEM "xmltv.dtd">\n')
        tree.write(f, encoding="unicode" if False else "UTF-8", xml_declaration=False)

    logging.info("[XMLTV] Generated %s with %d channels", output_path, len(channels))


def _xmltv_ts(dt: datetime) -> str:
    """Format datetime as XMLTV timestamp: YYYYMMDDHHmmss +0000."""
    return dt.strftime("%Y%m%d%H%M%S") + " +0000"
