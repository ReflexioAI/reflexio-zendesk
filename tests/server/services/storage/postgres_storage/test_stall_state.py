from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from psycopg2 import sql

from reflexio.server.services.storage.postgres_storage._stall_state import (
    PostgresStallStateMixin,
)


class FakePostgresStallStorage(PostgresStallStateMixin):
    def __init__(self, rows: list[list[dict[str, Any]]] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[list[Any]] = []

    def _table_identifier(self, name: str) -> sql.Composable:
        return sql.Identifier("public", name)

    def _fetch_all(
        self, query: sql.Composable, params: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        self.calls.append(params or [])
        return self.rows.pop(0) if self.rows else []


def test_get_stall_state_defaults_clean_when_row_missing() -> None:
    storage = FakePostgresStallStorage()

    state = storage.get_stall_state()

    assert state.stalled is False
    assert state.reason is None
    assert state.notified_in_cc is False


def test_get_stall_state_hydrates_row() -> None:
    now = datetime.now(UTC)
    storage = FakePostgresStallStorage(
        [
            [
                {
                    "stalled": True,
                    "reason": "billing_error",
                    "stalled_at": now,
                    "reset_estimate": None,
                    "notified_in_cc": False,
                    "error_message": "quota",
                }
            ]
        ]
    )

    state = storage.get_stall_state()

    assert state.stalled is True
    assert state.reason == "billing_error"
    assert state.stalled_at == now
    assert state.error_message == "quota"


def test_upsert_stall_state_writes_expected_params() -> None:
    now = datetime.now(UTC)
    storage = FakePostgresStallStorage()

    storage.upsert_stall_state(
        reason="auth_error",
        stalled_at=now,
        reset_estimate=None,
        error_message="login",
    )

    assert storage.calls == [["auth_error", now, None, now, "login"]]
