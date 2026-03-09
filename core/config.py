"""JSON-file-backed settings with env-var fallback."""

import json
import os
import logging

SETTINGS_FILE = os.getenv("SETTINGS_FILE", "/app/data/settings.json")

DEFAULTS = {
    "MEDIA_PATH": "/media",
    "BUMPS_PATH": "/bumps",
    "HLS_OUTPUT_PATH": "/app/data/hls",
    "M3U_OUTPUT_PATH": "/m3u",
    "HLS_TIME": "6",
    "HLS_LIST_SIZE": "10",
    "FFMPEG_LOGLEVEL": "warning",
    "VIDEO_PRESET": "fast",
    "VIDEO_CRF": "",
    "FFMPEG_THREADS": "1",
    "X264_THREADS": "4",
    "AUDIO_BITRATE": "192k",
    "BASE_URL": "http://192.168.20.34:5045",
    "LOG_FILE": "/app/logs/channelarr.log",
}


def _load_json() -> dict:
    try:
        with open(SETTINGS_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_json(data: dict):
    os.makedirs(os.path.dirname(SETTINGS_FILE), exist_ok=True)
    with open(SETTINGS_FILE, "w") as f:
        json.dump(data, f, indent=2)


def get_setting(key: str, default=None) -> str:
    """Read setting: JSON file -> env var -> DEFAULTS -> default arg."""
    file_data = _load_json()
    if key in file_data and file_data[key] != "":
        return str(file_data[key])
    env_val = os.environ.get(key)
    if env_val is not None:
        return env_val
    if key in DEFAULTS:
        return DEFAULTS[key]
    return default or ""


def get_all_settings() -> dict:
    """Return merged settings dict (JSON overrides env overrides defaults)."""
    result = dict(DEFAULTS)
    for key in DEFAULTS:
        env_val = os.environ.get(key)
        if env_val is not None:
            result[key] = env_val
    file_data = _load_json()
    for key, val in file_data.items():
        if val != "":
            result[key] = str(val)
    return result


def save_settings(data: dict):
    """Merge new settings into JSON file."""
    current = _load_json()
    current.update(data)
    _save_json(current)
    logging.info("[CONFIG] Settings saved to %s", SETTINGS_FILE)
