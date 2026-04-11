#!/bin/bash
set -e

# Start Xvfb virtual display
Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset &
XVFB_PID=$!

# Wait for Xvfb to be ready
sleep 1

# Trap to kill Xvfb on exit
trap "kill $XVFB_PID 2>/dev/null || true" EXIT

# Launch the FastAPI app
exec uvicorn app:app --host 0.0.0.0 --port 4445
