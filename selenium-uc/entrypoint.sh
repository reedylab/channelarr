#!/bin/bash
set -e

# Clean up any stale X11 socket from a previous run
rm -f /tmp/.X11-unix/X99

# Supervisor loop for Xvfb — if it dies, restart it
xvfb_supervisor() {
  while true; do
    Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset
    echo "[entrypoint] Xvfb exited, restarting in 1s..." >&2
    sleep 1
  done
}
xvfb_supervisor &
SUPERVISOR_PID=$!

# Wait for the X11 socket to appear (more reliable than a fixed sleep)
for i in $(seq 1 30); do
  if [ -S /tmp/.X11-unix/X99 ]; then
    echo "[entrypoint] Xvfb ready on :99"
    break
  fi
  sleep 0.2
done

# Kill the supervisor on exit
trap "kill $SUPERVISOR_PID 2>/dev/null || true; pkill -P $SUPERVISOR_PID 2>/dev/null || true" EXIT

# Launch the FastAPI app
exec uvicorn app:app --host 0.0.0.0 --port 4445
