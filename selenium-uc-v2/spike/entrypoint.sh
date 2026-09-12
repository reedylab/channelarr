#!/bin/bash
set -e

# Display number is configurable (default :99) because this spike gets run
# with --network container:channelarr-vpn to borrow gluetun's VPN egress for
# testing — which means it shares that netns's TCP ports too, and the real
# channelarr-selenium-uc container already has its own Xvfb bound to :99
# (TCP 6099) in that same shared namespace. Use DISPLAY_NUM=98 (or anything
# else free) when running alongside it.
DISPLAY_NUM="${DISPLAY_NUM:-99}"
export DISPLAY=":${DISPLAY_NUM}"

# Same Xvfb supervisor pattern as selenium-uc/entrypoint.sh — see that file's
# comments for why the lock-file cleanup and restart loop exist.
rm -f "/tmp/.X11-unix/X${DISPLAY_NUM}" "/tmp/.X${DISPLAY_NUM}-lock"

xvfb_supervisor() {
  set +e
  while true; do
    rm -f "/tmp/.X11-unix/X${DISPLAY_NUM}" "/tmp/.X${DISPLAY_NUM}-lock"
    Xvfb ":${DISPLAY_NUM}" -screen 0 1920x1080x24 -ac +extension GLX +render -noreset
    echo "[entrypoint] Xvfb exited (status=$?), restarting in 1s..." >&2
    sleep 1
  done
}
xvfb_supervisor &
SUPERVISOR_PID=$!

for i in $(seq 1 30); do
  if [ -S "/tmp/.X11-unix/X${DISPLAY_NUM}" ]; then
    echo "[entrypoint] Xvfb ready on :${DISPLAY_NUM}"
    break
  fi
  sleep 0.2
done

trap "kill $SUPERVISOR_PID 2>/dev/null || true; pkill -P $SUPERVISOR_PID 2>/dev/null || true" EXIT

exec python spike.py
