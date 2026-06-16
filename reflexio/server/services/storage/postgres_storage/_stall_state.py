"""Postgres CRUD for the singleton ``stall_state`` row."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any

from psycopg2 import sql

from reflexio.models.api_schema.stall_state_schema import StallReason
from reflexio.server.services.storage.postgres_storage._base import PostgresStorageBase
from reflexio.server.services.storage.sqlite_storage._stall_state import StallState

logger = logging.getLogger(__name__)
handle_exceptions = PostgresStorageBase.handle_exceptions


def _parse_ts(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    try:
        return datetime.fromisoformat(str(raw))
    except ValueError:
        logger.warning("Malformed timestamp in stall_state: %r", raw)
        return None


class PostgresStallStateMixin:
    """Postgres-backed stall_state operations exposed as storage methods."""

    _fetch_all: Callable[..., list[dict[str, Any]]]
    _table_identifier: Callable[[str], sql.Composable]

    @handle_exceptions
    def get_stall_state(self) -> StallState:
        rows = self._fetch_all(
            sql.SQL(
                """
                SELECT stalled, reason, stalled_at, reset_estimate,
                       notified_in_cc, error_message
                FROM {}
                WHERE id = 1
                """
            ).format(self._table_identifier("stall_state"))
        )
        if not rows:
            return StallState(False, None, None, None, False, None)
        row = rows[0]
        return StallState(
            stalled=bool(row["stalled"]),
            reason=row["reason"],
            stalled_at=_parse_ts(row["stalled_at"]),
            reset_estimate=_parse_ts(row["reset_estimate"]),
            notified_in_cc=bool(row["notified_in_cc"]),
            error_message=row["error_message"],
        )

    @handle_exceptions
    def upsert_stall_state(
        self,
        *,
        reason: StallReason,
        stalled_at: datetime,
        reset_estimate: datetime | None,
        error_message: str,
    ) -> None:
        self._fetch_all(
            sql.SQL(
                """
                INSERT INTO {} (
                    id, stalled, reason, stalled_at, reset_estimate,
                    notified_in_cc, last_attempt_at, error_message
                )
                VALUES (1, true, %s, %s, %s, false, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    stalled = EXCLUDED.stalled,
                    reason = EXCLUDED.reason,
                    stalled_at = EXCLUDED.stalled_at,
                    reset_estimate = EXCLUDED.reset_estimate,
                    notified_in_cc = false,
                    last_attempt_at = EXCLUDED.last_attempt_at,
                    error_message = EXCLUDED.error_message
                RETURNING 1
                """
            ).format(self._table_identifier("stall_state")),
            [reason, stalled_at, reset_estimate, stalled_at, error_message],
        )

    @handle_exceptions
    def mark_stall_notified(self) -> None:
        self._fetch_all(
            sql.SQL(
                """
                UPDATE {}
                SET notified_in_cc = true
                WHERE id = 1 AND stalled = true
                RETURNING 1
                """
            ).format(self._table_identifier("stall_state"))
        )

    @handle_exceptions
    def clear_stall_state(self) -> None:
        self._fetch_all(
            sql.SQL(
                """
                INSERT INTO {} (id, stalled, notified_in_cc)
                VALUES (1, false, false)
                ON CONFLICT (id) DO UPDATE SET
                    stalled = false,
                    reason = NULL,
                    stalled_at = NULL,
                    reset_estimate = NULL,
                    notified_in_cc = false,
                    error_message = NULL
                RETURNING 1
                """
            ).format(self._table_identifier("stall_state"))
        )
