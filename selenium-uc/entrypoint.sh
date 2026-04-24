#!/bin/bash
set -e

# Clean up any stale X11 socket AND lock from a previous run. The lock file
# (/tmp/.X99-lock) is what Xvfb actually checks to decide 'display in use';
# a stale one from a SIGKILL causes 'Server is already active for display 99'
# and Xvfb refuses to start. The socket alone isn't enough to clear.
rm -f /tmp/.X11-unix/X99 /tmp/.X99-lock

# Supervisor loop for Xvfb — if it dies, restart it. Clean the lock+socket
# before each attempt so a crash doesn't leave the supervisor stuck looping
# on 'already active'. Runs under `set +e` so a nonzero Xvfb exit (e.g.
# SIGKILL → 137) doesn't terminate the loop; `set -e` in the parent would
# otherwise kill the subshell on the first crash.
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
