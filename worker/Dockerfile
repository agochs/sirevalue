# Worker image — runs the nightly scraper + pushes fresh data back to the repo.
# Multi-stage to keep the final image small.

FROM python:3.11-slim AS base

# System deps for Playwright (chromium) and git
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    wget \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libexpat1 \
    libgbm1 \
    libglib2.0-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libx11-6 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (better caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
 && python -m playwright install chromium

# Copy the worker code. In practice you'd also COPY the existing pipeline files
# (scrapers.py, value_score.py, etc.) from the outputs/ directory at repo root.
COPY . /app

# Environment defaults (override via Fly.io secrets)
ENV PYTHONUNBUFFERED=1 \
    OUTPUT_DIR=/app/data \
    GIT_AUTHOR_NAME="SireValue Bot" \
    GIT_AUTHOR_EMAIL=bot@sirevalue.app

RUN mkdir -p /app/data
VOLUME ["/app/data"]

ENTRYPOINT ["/app/entrypoint.sh"]
