# Channelarr

A self-hosted custom TV channel builder. Create your own 24/7 live TV channels from your media library — movies, TV shows, and bump clips — served as HLS streams compatible with Jellyfin, Threadfin, or any IPTV client.

## What It Does

Channelarr lets you build TV channels that feel like real broadcast television. Add movies and shows to a channel, configure bumps (short interstitial clips like station idents or "up next" segments), and start streaming. The app handles scheduling, FFmpeg encoding, and HLS delivery.

Channels auto-start on demand when a client (like Jellyfin) requests the stream, and auto-stop when everyone stops watching.

## Features

- **Channel Builder** — Create channels from your movie and TV library. Add whole shows or individual episodes. Shuffle, loop, and configure per-channel.
- **Bump System** — Insert short clips between content (station idents, transitions, promos). Organize bumps into folders, download from YouTube via yt-dlp, and configure frequency, count, and placement per channel.
- **"Up Next" Overlays** — During bumps, display the next content's title and poster image with a countdown timer, just like real TV.
- **HLS Streaming** — Pipe-based FFmpeg architecture for seamless, gapless playback. No stuttering at file boundaries. Outputs standard HLS (`.m3u8` + `.ts` segments).
- **On-Demand Lifecycle** — Streams start automatically when a client requests the playlist and stop when all viewers disconnect (via Jellyfin log monitoring).
- **M3U + XMLTV Export** — Generates an M3U playlist and XMLTV EPG file for import into Jellyfin, Threadfin, or other IPTV frontends.
- **Channel Logos** — Upload PNG/JPEG logos per channel. Included in M3U and EPG output.
- **Media Browser** — Scan your filesystem for movies and TV shows. Poster art detected automatically from sidecar files.
- **Web UI** — Dark-themed single-page app with channel cards, media picker, bump manager, live log tail, system stats (CPU/RAM/disk charts), and settings editor.
- **Configurable Encoding** — x264 preset, CRF, thread count, audio bitrate — all tunable from the settings page.

## Screenshots

The web UI shows channel cards with full-width logo images, live/off status badges, and quick-action buttons for start, stop, watch, edit, and delete.

## Quick Start

### Docker Compose

```yaml
services:
  channelarr:
    build: .
    ports:
      - "5045:5045"
    volumes:
      - /path/to/media:/media:ro
      - /path/to/bumps:/bumps
      - /path/to/m3u:/m3u
      - channelarr-data:/app/data
      - channelarr-logs:/app/logs
    environment:
      - BASE_URL=http://your-server-ip:5045

volumes:
  channelarr-data:
  channelarr-logs:
```

```bash
docker compose up -d
```

Open `http://your-server-ip:5045` in your browser.

## Usage

### 1. Create a Channel

Click **New Channel**, give it a name, and add content from your media library. Optionally upload a logo and configure bumps.

### 2. Configure Bumps (Optional)

Place short video clips in folders under your bumps path. In the channel editor, enable bumps, select folders, and set frequency (between every item, or every N items). Enable "Show Next" to overlay upcoming content info during bumps.

### 3. Start Streaming

Click **Start** on a channel card, or let it auto-start when a client requests the HLS playlist. The stream URL is:

```
http://your-server-ip:5045/live/<channel-id>/stream.m3u8
```

### 4. Add to Jellyfin

Import the generated M3U playlist (`/m3u/channelarr.m3u`) and XMLTV guide (`/m3u/channelarr.xml`) into Jellyfin's Live TV settings, or feed them through Threadfin.

## Configuration

All settings are configurable from the web UI under **Settings**:

| Setting | Default | Description |
|---------|---------|-------------|
| MEDIA_PATH | `/media` | Root path to your media library |
| BUMPS_PATH | `/bumps` | Root path to bump clip folders |
| HLS_OUTPUT_PATH | `/app/data/hls` | Where HLS segments are written |
| M3U_OUTPUT_PATH | `/m3u` | Where M3U and XMLTV files are written |
| HLS_TIME | `6` | HLS segment duration in seconds |
| HLS_LIST_SIZE | `10` | Number of segments in the playlist window |
| VIDEO_PRESET | `fast` | x264 encoding preset |
| VIDEO_CRF | codec default | Constant Rate Factor (quality) |
| FFMPEG_THREADS | `1` | FFmpeg threads |
| X264_THREADS | `4` | x264 encoder threads |
| AUDIO_BITRATE | `192k` | AAC audio bitrate |
| BASE_URL | `http://192.168.20.34:5045` | Public base URL for M3U/EPG links |

## API

All endpoints are under `/api`:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/status` | Channel count and running count |
| GET | `/api/channels` | List all channels with status |
| POST | `/api/channels` | Create a new channel |
| GET | `/api/channels/<id>` | Get channel details + schedule preview |
| PUT | `/api/channels/<id>` | Update channel configuration |
| DELETE | `/api/channels/<id>` | Delete channel |
| POST | `/api/channels/<id>/start` | Start streaming |
| POST | `/api/channels/<id>/stop` | Stop streaming |
| GET | `/api/media/movies` | List movies |
| GET | `/api/media/tv` | List TV shows |
| GET | `/api/media/tv/episodes?path=...` | List episodes for a show |
| GET | `/api/bumps` | List all bump folders and clips |
| POST | `/api/bumps/scan` | Rescan bumps directory |
| POST | `/api/bumps/download` | Download a video as a bump clip |
| GET | `/api/settings` | Get settings schema and values |
| POST | `/api/settings` | Save settings |
| GET | `/api/system/stats` | CPU, RAM, disk stats + 24h history |
| POST | `/api/m3u/regenerate` | Regenerate M3U playlist and EPG |

## Architecture

```
channelarr/
  core/
    channels.py    — Channel CRUD, scheduling, concat file generation
    streamer.py    — FFmpeg HLS streaming engine (pipe-based)
    bumps.py       — Bump clip scanning, cycling, YouTube downloads
    media.py       — Media library filesystem scanner
    config.py      — JSON-backed settings with env fallback
    nfo.py         — NFO metadata parsing, poster discovery
    xmltv.py       — XMLTV EPG generation
  web/
    app.py         — Flask app factory
    blueprints/
      ui.py        — Web UI route
      api.py       — REST API endpoints
      hls.py       — HLS playlist/segment serving + auto-start
    static/        — CSS, JS
    templates/     — HTML
```

The streaming pipeline: content files are read by FFmpeg, encoded to MPEG-TS, and piped into an HLS segmenter. The pipe never breaks between files, so playback is seamless with no gaps or glitches at transitions.

## Requirements

- Docker
- A media library (movies/TV shows as video files)
- FFmpeg (included in the Docker image)

## License

MIT
