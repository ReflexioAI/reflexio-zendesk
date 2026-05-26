"""Environment helpers for native Postgres storage configuration."""

from __future__ import annotations

import os


def postgres_db_url_from_env() -> str:
    """Return the configured Postgres URL.

    ``POSTGRES_DB_URL`` is the self-host/deployment-facing name, matching
    reflexio-enterprise. ``REFLEXIO_POSTGRES_DB_URL`` remains supported as the
    lower-level OSS config-writer name and wins when both are set.
    """
    return (
        os.environ.get("REFLEXIO_POSTGRES_DB_URL")
        or os.environ.get("POSTGRES_DB_URL")
        or ""
    ).strip()


def postgres_pool_size_from_env(default: int = 5) -> int:
    """Return the configured Postgres pool size, falling back to *default*."""
    pool_size_raw = os.environ.get("REFLEXIO_POSTGRES_POOL_SIZE", "").strip()
    return int(pool_size_raw) if pool_size_raw.isdigit() else default
