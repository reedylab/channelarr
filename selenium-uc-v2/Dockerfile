# Sidecar 2.0 — Phase 1 real service. Deployed as a brand-new, separately-run
# container (never replaces selenium-uc/ during validation — see the
# sidecar-2.0 plan's migration/rollout section). No chromedriver-patching
# complexity here (nodriver drives Chrome directly over CDP) — a genuine
# simplification over selenium-uc/Dockerfile.
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libnss3 \
    libxss1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    libdrm2 \
    libgbm1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libxkbcommon0 \
    libu2f-udev \
    libvulkan1 \
    xdg-utils \
    xvfb \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Real Google Chrome, not Debian's chromium (same reasoning as selenium-uc/
# Dockerfile — some CDN/anti-bot fingerprinting distinguishes them).
RUN wget -q -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y /tmp/chrome.deb \
    && rm /tmp/chrome.deb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Homelab vendor patch (same as the Phase 0 spike's Dockerfile, same
# reasoning, lifted from stealth-browser-mcp's own Dockerfile — same author):
# nodriver's CDP-connect retry window defaults to range(5) (~2.75s). Headful
# Chrome cold-starting through gluetun's VPN network namespace on a small/
# loaded box exposes its DevTools port slower than that. range(40) ~= 20s
# covers it.
RUN python - <<'PY'
import nodriver.core.browser as m
f = m.__file__
s = open(f).read()
s = s.replace("        for _ in range(5):", "        for _ in range(40):")
s = s.replace("                if _ == 4:", "                if _ == 39:")
open(f, "w").write(s)
assert "range(40)" in s, "nodriver connect-window patch did not apply"
print("nodriver connect-window patched:", f)
PY

COPY app.py network_capture.py session_attach.py entrypoint.sh ./
RUN chmod +x entrypoint.sh

ENV DISPLAY=:99
EXPOSE 4446
CMD ["./entrypoint.sh"]
