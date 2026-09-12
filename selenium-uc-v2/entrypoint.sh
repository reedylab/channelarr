#!/bin/bash
set -e

# Same Xvfb supervisor pattern as selenium-uc/entrypoint.sh — see that file's
# comments for why the lock-file cleanup and restart loop exist.
rm -f /tmp/.X11-unix/X99 /tmp/.X99-lock

xvfb_supervisor() {
  set +e
  while true; do
    rm -f /tmp/.X11-unix/X99 /tmp/.X99-lock
    Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset
    echo "[entrypoint] Xvfb exited (status=$?), restarting in 1s..." >&2
    sleep 1
  done
}
xvfb_supervisor &
SUPERVISOR_PID=$!

for i in $(seq 1 30); do
  if [ -S /tmp/.X11-unix/X99 ]; then
    echo "[entrypoint] Xvfb ready on :99"
    break
  fi
  sleep 0.2
done

trap "kill $SUPERVISOR_PID 2>/dev/null || true; pkill -P $SUPERVISOR_PID 2>/dev/null || true" EXIT

exec uvicorn app:app --host 0.0.0.0 --port 4446
