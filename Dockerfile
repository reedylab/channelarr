FROM python:3.11-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg fonts-dejavu-core nodejs libcairo2 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x entrypoint.sh

CMD ["./entrypoint.sh"]
