# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_CACHE_DIR=/tmp/uv-cache \
    PATH="/app/.venv/bin:${PATH}" \
    BACKEND_PORT=8081 \
    LOCAL_STORAGE_PATH=/data/reflexio \
    REFLEXIO_LOG_DIR=/data/reflexio \
    REFLEXIO_STORAGE=sqlite

WORKDIR /app

COPY pyproject.toml uv.lock README.md LICENSE .env.example ./
COPY reflexio ./reflexio

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && uv sync --frozen --no-dev --no-editable \
    && rm -rf "$UV_CACHE_DIR" \
    && apt-get purge -y --auto-remove build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 reflexio \
    && useradd --system --uid 10001 --gid reflexio --home-dir /home/reflexio --create-home reflexio \
    && mkdir -p /data/reflexio "$UV_CACHE_DIR" \
    && chown -R reflexio:reflexio /app /data/reflexio "$UV_CACHE_DIR" /home/reflexio

ENV UV_NO_SYNC=1

USER reflexio

EXPOSE 8081

CMD ["uv", "run", "reflexio", "services", "start", "--only", "backend", "--no-reload", "--workers", "1", "--storage", "sqlite"]
