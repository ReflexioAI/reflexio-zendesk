from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from psycopg2 import sql

from reflexio.models.api_schema.eval_overview_schema import (
    ShadowComparisonOutput,
    ShadowComparisonVerdict,
)
from reflexio.server.services.storage.postgres_storage._shadow_verdicts import (
    PostgresShadowVerdictsMixin,
)


class FakePostgresShadowStorage(PostgresShadowVerdictsMixin):
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


def _row(**overrides: Any) -> dict[str, Any]:
    row = {
        "verdict_id": 7,
        "interaction_id": "i1",
        "session_id": "s1",
        "agent_version": "v1",
        "reflexio_is_request_1": True,
        "better_request": "1",
        "is_significantly_better": True,
        "comparison_reason": "clearer",
        "judge_prompt_version": "v1.0.0",
        "created_at": datetime.now(UTC),
    }
    row.update(overrides)
    return row


def _verdict(**overrides: Any) -> ShadowComparisonVerdict:
    data = {
        "verdict_id": 0,
        "interaction_id": "i1",
        "session_id": "s1",
        "agent_version": "v1",
        "reflexio_is_request_1": True,
        "output": ShadowComparisonOutput(
            better_request="1",
            is_significantly_better=True,
            comparison_reason="clearer",
        ),
        "judge_prompt_version": "v1.0.0",
        "created_at": datetime.now(UTC),
    }
    data.update(overrides)
    return ShadowComparisonVerdict(**data)


def test_save_shadow_comparison_verdict_returns_inserted_row() -> None:
    storage = FakePostgresShadowStorage([[_row(verdict_id=11)]])
    verdict = _verdict()

    saved = storage.save_shadow_comparison_verdict(verdict)

    assert saved.verdict_id == 11
    assert storage.calls[0][0:3] == ["i1", "s1", "v1"]
    assert storage.calls[0][4:8] == ["1", True, "clearer", "v1.0.0"]


def test_get_shadow_comparison_verdict_returns_none_for_missing_id() -> None:
    storage = FakePostgresShadowStorage([[]])

    assert storage.get_shadow_comparison_verdict(123) is None
    assert storage.calls == [[123]]


def test_get_shadow_comparison_verdicts_hydrates_rows() -> None:
    storage = FakePostgresShadowStorage(
        [[_row(interaction_id="earlier"), _row(interaction_id="later")]]
    )

    verdicts = storage.get_shadow_comparison_verdicts(
        from_ts=10,
        to_ts=20,
        judge_prompt_version="v1.0.0",
    )

    assert [verdict.interaction_id for verdict in verdicts] == ["earlier", "later"]
    assert storage.calls[0][2] == "v1.0.0"


def test_delete_shadow_comparison_verdicts_by_session_returns_count() -> None:
    storage = FakePostgresShadowStorage([[{"?column?": 1}, {"?column?": 1}]])

    assert storage.delete_shadow_comparison_verdicts_by_session("s1") == 2
    assert storage.calls == [["s1"]]
