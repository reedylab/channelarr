#!/bin/bash
set -e

mkdir -p /app/logs /app/data /app/data/hls

exec gunicorn -c web/gunicorn.config.py web.app:app
