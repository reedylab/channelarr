#!/bin/bash
set -e

mkdir -p /app/logs /app/data /app/data/hls

exec uvicorn web.app:app --host 0.0.0.0 --port 5045 --workers 1 --timeout-keep-alive 75
