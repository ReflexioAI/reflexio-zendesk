from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from psycopg2 import sql

from reflexio.models.api_schema.braintrust_schema import (
    BraintrustConnection,
    ImportedScore,
)
from reflexio.server.services.storage.postgres_storage._extras import ExtrasMixin


class FakePostgresExtrasStorage(ExtrasMixin):
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

    def _parse_datetime_to_timestamp(self, value: datetime | str) -> int:
        if isinstance(value, datetime):
            return int(value.timestamp())
        return int(datetime.fromisoformat(value).timestamp())


def _interaction_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "interaction_id": 101,
        "user_id": "u1",
        "content": "hello",
        "request_id": "r1",
        "created_at": datetime.now(UTC).isoformat(),
        "role": "User",
        "user_action": "none",
        "user_action_description": "",
        "interacted_image_url": "",
        "shadow_content": "",
        "expert_content": "",
        "tools_used": [],
        "citations": [
            {
                "kind": "playbook",
                "real_id": "42",
                "title": "Check the logs",
            }
        ],
    }
    row.update(overrides)
    return row


def test_get_interactions_by_session_hydrates_citations() -> None:
    storage = FakePostgresExtrasStorage([[_interaction_row()]])

    interactions = storage.get_interactions_by_session("s1")

    assert interactions[0].citations[0].real_id == "42"
    assert storage.calls == [["s1"]]


def test_get_playbook_application_stats_deduplicates_per_interaction() -> None:
    now = datetime.now(UTC)
    storage = FakePostgresExtrasStorage(
        [
            [
                {
                    "interaction_id": 2,
                    "created_at": now,
                    "citations": [
                        {"kind": "playbook", "real_id": "p1", "title": "Rule"},
                        {"kind": "playbook", "real_id": "p1", "title": "Rule"},
                    ],
                },
                {
                    "interaction_id": 1,
                    "created_at": now,
                    "citations": [
                        {"kind": "profile", "real_id": "prof1", "title": "Fact"}
                    ],
                },
            ]
        ]
    )

    stats = storage.get_playbook_application_stats(days_back=30)

    assert [(stat.kind, stat.real_id, stat.applied_count) for stat in stats] == [
        ("playbook", "p1", 1),
        ("profile", "prof1", 1),
    ]


def test_count_sessions_with_shadow_content_returns_count() -> None:
    storage = FakePostgresExtrasStorage([[{"n": 3}]])

    assert storage.count_sessions_with_shadow_content(10, 20) == 3


def test_braintrust_connection_roundtrips_row() -> None:
    storage = FakePostgresExtrasStorage(
        [
            [],
            [
                {
                    "api_key_enc": "enc",
                    "workspace_id": "ws",
                    "workspace_name": "Workspace",
                    "project_ids": ["p1"],
                    "last_sync_ts": 123,
                    "last_error": None,
                }
            ],
        ]
    )
    connection = BraintrustConnection(
        org_id="org",
        api_key_enc="enc",
        workspace_id="ws",
        workspace_name="Workspace",
        project_ids=["p1"],
    )

    storage.save_braintrust_connection(connection)
    loaded = storage.get_braintrust_connection("org")

    assert loaded == connection.model_copy(update={"last_sync_ts": 123})


def test_imported_scores_roundtrip_rows() -> None:
    storage = FakePostgresExtrasStorage(
        [
            [],
            [
                {
                    "source": "braintrust",
                    "source_run_id": "run",
                    "session_id": "session",
                    "scorer_name": "score",
                    "value": 0.75,
                    "ts": 123,
                }
            ],
        ]
    )

    storage.save_imported_scores(
        [
            ImportedScore(
                org_id="org",
                source_run_id="run",
                session_id="session",
                scorer_name="score",
                value=0.75,
                ts=123,
            )
        ]
    )
    scores = storage.get_imported_scores("org", 0, 200)

    assert scores == [
        ImportedScore(
            org_id="org",
            source_run_id="run",
            session_id="session",
            scorer_name="score",
            value=0.75,
            ts=123,
        )
    ]
