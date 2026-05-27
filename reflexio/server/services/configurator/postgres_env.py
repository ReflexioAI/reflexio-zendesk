"""Environment helpers for native Postgres storage configuration."""

from __future__ import annotations

import os

from reflexio.models.config_schema import PostgresSearchBackend


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


def postgres_search_backend_from_env(
    default: PostgresSearchBackend = PostgresSearchBackend.POSTGRES,
) -> PostgresSearchBackend:
    """Return the configured search backend for native Postgres storage."""
    raw = os.environ.get("REFLEXIO_POSTGRES_SEARCH_BACKEND", "").strip().lower()
    if not raw:
        return default
    try:
        return PostgresSearchBackend(raw)
    except ValueError as exc:
        allowed = ", ".join(backend.value for backend in PostgresSearchBackend)
        raise ValueError(
            f"REFLEXIO_POSTGRES_SEARCH_BACKEND must be one of: {allowed}"
        ) from exc
