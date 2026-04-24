FROM python:3.11-slim

# Install required libs + tools to add Google Chrome repo
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
    && rm -rf /var/lib/apt/lists/*

# Install real Google Chrome (PerimeterX/Akamai fingerprint Debian's chromium differently)
RUN wget -q -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y /tmp/chrome.deb \
    && rm /tmp/chrome.deb \
    && rm -rf /var/lib/apt/lists/*

# Let undetected_chromedriver auto-download a matching chromedriver
ENV CHROME_BINARY=/usr/bin/google-chrome

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py tab_proxy.py entrypoint.sh ./
RUN chmod +x entrypoint.sh

ENV DISPLAY=:99
EXPOSE 4445
CMD ["./entrypoint.sh"]
