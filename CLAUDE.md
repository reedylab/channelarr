# Channelarr

Self-hosted 24/7 custom TV channel builder. Mixes local media (movies, TV shows) and YouTube content into persistent EPG schedules, served as on-demand HLS streams compatible with Jellyfin, Threadfin, Manifold, or any IPTV client. Background cache worker keeps the next-N YouTube videos pre-downloaded so streams start instantly.

## Deploy

- **Primary host:** `home-media01`
- **Compose file:** `docker-compose.yml` (gluetun + channelarr + searxng + selenium-uc)
- **Run:** `docker compose up -d --build`
- **Port:** `5045` (HLS streams + web UI; bound on the `gluetun` container — `channelarr` itself is `network_mode: service:gluetun`)
- **Containers:** `channelarr-vpn` (gluetun), `channelarr`, `channelarr-searxng`, `channelarr-selenium-uc`

## Architecture

- Python + FastAPI core in `core/`, scrapers in `scrapers/`, web UI in `web/`
- Postgres: **shared instance** at `192.168.20.15:5432`, DB `channelarr`, user `channelarr` (set `PG_PASS` in `.env`)
- VPN: routed through gluetun sidecar (Mullvad WireGuard) — `WG_PRIVATE_KEY` + `VPN_CONTROL_PASS` required
- Media volumes: `/mnt/das-disk-1/media:ro`, bumps under `/mnt/das-disk-1/media/bumps`, YouTube cache under `/home/jake/media/youtube`

## Test / validation

- `curl http://192.168.20.34:5045/api/channels` should return JSON
- M3U URL for clients: `http://192.168.20.34:5045/live/<channel-id>/stream.m3u8`
- XMLTV export endpoint: `/xmltv.xml`

## Gotchas

- All host-facing ports MUST be declared on the **gluetun** service in compose, not on `channelarr` — `network_mode: service:gluetun` means channelarr has no own network namespace.
- YouTube cache is bounded but can balloon transiently; if disk pressure shows up, the cache worker is the first place to check.
- `searxng` is a co-dependency of the scrapers — don't strip it casually.

## Project-specific git conventions

None beyond global.
