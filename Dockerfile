# Stage 1: builder — install Python deps (gcc only needed here for native wheels)
FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: runtime — slim image without build toolchain
FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY DATA_MODEL.md .
COPY config/ ./config/
COPY scripts/ ./scripts/
COPY app/ ./app/

RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser
ENV HOME=/home/appuser

# Cloud Run injects PORT; 8080 is its default
ENV PORT=8080
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import os, urllib.request; urllib.request.urlopen('http://localhost:' + os.environ.get('PORT', '8080') + '/health')" || exit 1

CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
