"""Generate XMLTV EPG for Channelarr channels using materialized schedules.

Each channel's materialized_schedule contains entries with real start/stop
timestamps. For looping channels the schedule is projected forward to fill
a configurable time window (default 48 hours).
"""

import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from xml.etree.ElementTree import Element, SubElement, ElementTree, indent

WINDOW_HOURS = 48


def generate_channelarr_xmltv(channels: list, output_path: str, base_url: str):
    """Write channelarr.xml with real programme data from materialized schedules."""
    tv = Element("tv", attrib={
        "generator-info-name": "Channelarr",
        "generator-info-url": base_url,
    })

    now = datetime.now(timezone.utc)
    horizon = now + timedelta(hours=WINDOW_HOURS)

    # Channel definitions
    for ch in channels:
        cid = ch["id"]
        name = ch["name"]
        chan_el = SubElement(tv, "channel", id=cid)
        dn = SubElement(chan_el, "display-name", lang="en")
        dn.text = name
        logo_url = f"{base_url}/api/logo/{cid}"
        SubElement(chan_el, "icon", src=logo_url)

    # Programme entries
    total_programmes = 0
    for ch in channels:
        cid = ch["id"]
        # Resolved channels are pure live streams — no real schedule. Generate
        # placeholder blocks so guide consumers see the channel as populated
        # rather than empty. The block boundaries match the API's
        # current_placeholder_block() helper so the EPG and the channel tile
        # always agree on which block is current.
        if ch.get("type") == "resolved":
            ev_start = ch.get("event_start")
            ev_end = ch.get("event_end")
            if ev_start and ev_end:
                total_programmes += _generate_event_programme(
                    tv, cid, ch, now, horizon, base_url, ev_start, ev_end
                )
            else:
                total_programmes += _generate_placeholder_programmes(
                    tv, cid, ch["name"], now, horizon, base_url, is_live=True
                )
            continue

        schedule = ch.get("materialized_schedule", [])
        epoch_str = ch.get("schedule_epoch")
        cycle_dur = ch.get("schedule_cycle_duration", 0)

        if not schedule or not epoch_str or cycle_dur <= 0:
            # No schedule — generate placeholder blocks
            total_programmes += _generate_placeholder_programmes(
                tv, cid, ch["name"], now, horizon, base_url
            )
            continue

        # Collect content entries, absorbing bump gaps so programmes are seamless
        content_entries = _merge_bump_gaps(
            _iterate_schedule_window(schedule, epoch_str, cycle_dur,
                                      ch.get("loop", True), now, horizon)
        )

        count = 0
        for entry in content_entries:
            prog = SubElement(tv, "programme", attrib={
                "start": _xmltv_ts(entry["start"]),
                "stop": _xmltv_ts(entry["stop"]),
                "channel": cid,
            })

            title_el = SubElement(prog, "title", lang="en")
            title_el.text = entry.get("title", "")

            desc = entry.get("desc", "")
            if desc:
                desc_el = SubElement(prog, "desc", lang="en")
                desc_el.text = desc

            cat_el = SubElement(prog, "category", lang="en")
            if entry["type"] == "youtube":
                cat_el.text = "YouTube"
            elif entry["type"] == "episode":
                cat_el.text = "Series"
            else:
                cat_el.text = "Movie"

            if entry.get("thumbnail"):
                SubElement(prog, "icon", src=entry["thumbnail"])
            else:
                logo_url = f"{base_url}/api/logo/{cid}"
                SubElement(prog, "icon", src=logo_url)

            count += 1

        total_programmes += count

    indent(tv, space="  ")

    # Atomic write
    dir_name = os.path.dirname(output_path)
    os.makedirs(dir_name, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write(b'<!DOCTYPE tv SYSTEM "xmltv.dtd">\n')
            tree = ElementTree(tv)
            tree.write(f, encoding="UTF-8", xml_declaration=False)
        os.replace(tmp_path, output_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    logging.info("[XMLTV] Generated %s: %d channels, %d programmes",
                 output_path, len(channels), total_programmes)


def _merge_bump_gaps(entries_iter):
    """Filter out bumps and extend each content entry's stop to the next content start.

    This eliminates gaps in EPG/guide data caused by bump clips between programmes.
    """
    content = []
    for entry in entries_iter:
        if entry["type"] == "bump":
            continue
        content.append(dict(entry))

    # Extend each entry's stop to the next entry's start
    for i in range(len(content) - 1):
        content[i]["stop"] = content[i + 1]["start"]

    return content


def _iterate_schedule_window(schedule: list, epoch_str: str, cycle_dur: float,
                              loop: bool, window_start: datetime, window_end: datetime):
    """Yield schedule entries (with projected timestamps) that overlap a time window.

    For looping channels, the schedule is repeated forward from the epoch,
    yielding entries until we exceed window_end.
    """
    epoch = datetime.fromisoformat(epoch_str)

    if not loop:
        # Non-looping: just yield entries that fall within the window
        for entry in schedule:
            entry_start = datetime.fromisoformat(entry["start"])
            entry_stop = datetime.fromisoformat(entry["stop"])
            if entry_stop <= window_start:
                continue
            if entry_start >= window_end:
                break
            yield {
                **entry,
                "start": entry_start,
                "stop": entry_stop,
            }
        return

    # Looping: calculate which cycle iteration contains window_start
    elapsed_to_window_start = (window_start - epoch).total_seconds()
    if elapsed_to_window_start < 0:
        start_cycle = 0
    else:
        start_cycle = int(elapsed_to_window_start // cycle_dur)

    # Start one cycle earlier to catch entries that span the window boundary
    start_cycle = max(0, start_cycle - 1)

    cycle_num = start_cycle
    max_cycles = start_cycle + int((WINDOW_HOURS * 3600) / cycle_dur) + 3

    while cycle_num <= max_cycles:
        cycle_offset = timedelta(seconds=cycle_num * cycle_dur)
        for entry in schedule:
            entry_start = datetime.fromisoformat(entry["start"]) + cycle_offset - (epoch - epoch)
            # Recalculate from epoch + cycle offset + entry offset within cycle
            original_start = datetime.fromisoformat(entry["start"])
            offset_in_cycle = (original_start - epoch).total_seconds()
            projected_start = epoch + timedelta(seconds=cycle_num * cycle_dur + offset_in_cycle)
            projected_stop = projected_start + timedelta(seconds=entry["duration"])

            if projected_stop <= window_start:
                continue
            if projected_start >= window_end:
                return

            yield {
                **entry,
                "start": projected_start,
                "stop": projected_stop,
            }
        cycle_num += 1


def _generate_placeholder_programmes(tv_element, channel_id: str, channel_name: str,
                                      start: datetime, end: datetime, base_url: str,
                                      *, is_live: bool = False) -> int:
    """Generate 30-minute placeholder blocks for channels without a schedule.

    Block boundaries align to :00 and :30 of every hour so the EPG matches
    the API's current_placeholder_block() helper. is_live=True flavors the
    text and category for resolved channels (live streams) instead of
    scheduled-but-empty channels.
    """
    block = timedelta(minutes=30)
    # Align to the nearest :00 or :30 boundary at or before `start`
    block_start_minute = (start.minute // 30) * 30
    current = start.replace(minute=block_start_minute, second=0, microsecond=0)
    count = 0
    while current < end:
        stop = current + block
        prog = SubElement(tv_element, "programme", attrib={
            "start": _xmltv_ts(current),
            "stop": _xmltv_ts(stop),
            "channel": channel_id,
        })
        title_el = SubElement(prog, "title", lang="en")
        title_el.text = channel_name
        desc_el = SubElement(prog, "desc", lang="en")
        desc_el.text = f"{channel_name} — Live Stream" if is_live else f"{channel_name} — Scheduled Programming"
        cat_el = SubElement(prog, "category", lang="en")
        cat_el.text = "News" if is_live else "General"
        logo_url = f"{base_url}/api/logo/{channel_id}"
        SubElement(prog, "icon", src=logo_url)
        current = stop
        count += 1
    return count


def _get_epg_tz():
    """Return the configured EPG display timezone."""
    from core.config import get_setting
    tz_name = get_setting("EPG_TIMEZONE", "America/New_York")
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(tz_name)
    except Exception:
        return timezone.utc


def _format_local_time(dt, tz) -> str:
    """Format a datetime as a readable local time string like '7:15 PM'."""
    local = dt.astimezone(tz)
    return local.strftime("%-I:%M %p")


def _generate_event_programme(tv_element, channel_id: str, ch: dict,
                              window_start, window_end, base_url: str,
                              ev_start_str: str, ev_end_str: str) -> int:
    """Generate descriptive EPG blocks for an event channel.

    Three phases:
      - Before event: "Starts at 7:15 PM" in 30-min blocks
      - During event: single block with event title
      - After event:  "Event Ended" in 30-min blocks
    """
    ev_start = datetime.fromisoformat(ev_start_str)
    ev_end = datetime.fromisoformat(ev_end_str)
    name = ch["name"]
    tags = ch.get("tags") or []
    category = tags[0] if tags else "Event"
    logo_url = f"{base_url}/api/logo/{channel_id}"
    epg_tz = _get_epg_tz()
    local_start = _format_local_time(ev_start, epg_tz)
    count = 0

    # ── Pre-event blocks: "Starts at 7:15 PM" ──
    if ev_start > window_start:
        pre_end = min(ev_start, window_end)
        block = timedelta(minutes=30)
        block_start_minute = (window_start.minute // 30) * 30
        current = window_start.replace(minute=block_start_minute, second=0, microsecond=0)
        while current < pre_end:
            stop = min(current + block, pre_end)
            prog = SubElement(tv_element, "programme", attrib={
                "start": _xmltv_ts(current),
                "stop": _xmltv_ts(stop),
                "channel": channel_id,
            })
            title_el = SubElement(prog, "title", lang="en")
            title_el.text = f"{name} — Starts at {local_start}"
            desc_el = SubElement(prog, "desc", lang="en")
            desc_el.text = f"{name} begins at {local_start}."
            cat_el = SubElement(prog, "category", lang="en")
            cat_el.text = category
            SubElement(prog, "icon", src=logo_url)
            current = stop
            count += 1

    # ── Event block ──
    block_start = max(ev_start, window_start)
    block_end = min(ev_end, window_end)
    if block_start < block_end:
        prog = SubElement(tv_element, "programme", attrib={
            "start": _xmltv_ts(block_start),
            "stop": _xmltv_ts(block_end),
            "channel": channel_id,
        })
        title_el = SubElement(prog, "title", lang="en")
        title_el.text = name
        desc_el = SubElement(prog, "desc", lang="en")
        desc_el.text = f"{name} — Live"
        cat_el = SubElement(prog, "category", lang="en")
        cat_el.text = category
        SubElement(prog, "icon", src=logo_url)
        count += 1

    # ── Post-event blocks: "Event Ended" ──
    if ev_end < window_end:
        post_start = max(ev_end, window_start)
        block = timedelta(minutes=30)
        block_start_minute = (post_start.minute // 30) * 30
        current = post_start.replace(minute=block_start_minute, second=0, microsecond=0)
        if current < post_start:
            current += block
        # Use ev_end directly if it doesn't align to a block boundary
        if current > post_start:
            current = post_start
        while current < window_end:
            stop = current + block
            prog = SubElement(tv_element, "programme", attrib={
                "start": _xmltv_ts(current),
                "stop": _xmltv_ts(stop),
                "channel": channel_id,
            })
            title_el = SubElement(prog, "title", lang="en")
            title_el.text = f"{name} — Event Ended"
            desc_el = SubElement(prog, "desc", lang="en")
            desc_el.text = f"{name} has ended."
            cat_el = SubElement(prog, "category", lang="en")
            cat_el.text = category
            SubElement(prog, "icon", src=logo_url)
            current = stop
            count += 1

    return count


def _xmltv_ts(dt) -> str:
    """Format datetime as XMLTV timestamp: YYYYMMDDHHmmss +0000."""
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    return dt.strftime("%Y%m%d%H%M%S") + " +0000"
