"""Live Postgres storage smoke test.

Skipped unless POSTGRES_TEST_DB_URL, POSTGRES_DB_URL, or REFLEXIO_POSTGRES_DB_URL
points at a running Postgres database.
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import psycopg2
import pytest
from psycopg2 import sql

from reflexio.models.api_schema.service_schemas import (
    NEVER_EXPIRES_TIMESTAMP,
    ProfileTimeToLive,
    Request,
    UserProfile,
)
from reflexio.models.config_schema import StorageConfigPostgres
from reflexio.server.services.storage.postgres_storage import PostgresStorage
from reflexio.server.services.storage.storage_base import (
    AgentBinding,
    AgentRunRecord,
    AgentRunStatus,
    PendingToolCallRecord,
    PendingToolCallStatus,
    RunToolDependencyRecord,
    build_pending_tool_call_dedup_key,
    build_scope_hash,
    human_feedback_scope,
)
from tests.server.test_utils import skip_in_precommit


def _postgres_db_url() -> str:
    return (
        os.environ.get("POSTGRES_TEST_DB_URL")
        or os.environ.get("POSTGRES_DB_URL")
        or os.environ.get("REFLEXIO_POSTGRES_DB_URL")
        or ""
    )


@pytest.fixture
def postgres_storage() -> Generator[PostgresStorage]:
    db_url = _postgres_db_url()
    if not db_url:
        pytest.skip(
            "Set POSTGRES_TEST_DB_URL, POSTGRES_DB_URL, or REFLEXIO_POSTGRES_DB_URL"
        )

    schema = f"e2e_{uuid.uuid4().hex[:12]}"
    storage = PostgresStorage(
        org_id=f"postgres-e2e-{schema}",
        config=StorageConfigPostgres(db_url=db_url, schema=schema, pool_size=2),
    )
    try:
        with patch.object(storage, "_get_embedding", return_value=[0.0] * 512):
            yield storage
    finally:
        storage.close()
        with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


@skip_in_precommit
def test_postgres_storage_round_trip(postgres_storage: PostgresStorage) -> None:
    run_id = uuid.uuid4().hex[:8]
    user_id = f"pg-user-{run_id}"
    request_id = f"pg-request-{run_id}"
    now = int(time.time())

    postgres_storage.add_request(
        Request(
            request_id=request_id,
            user_id=user_id,
            created_at=now,
            source="docker-postgres-e2e",
            agent_version="codex",
            session_id=f"session-{run_id}",
        )
    )

    profile = UserProfile(
        profile_id=f"pg-profile-{run_id}",
        user_id=user_id,
        content="Prefers Docker Postgres with pgvector for local Reflexio tests.",
        last_modified_timestamp=now,
        generated_from_request_id=request_id,
        profile_time_to_live=ProfileTimeToLive.INFINITY,
        expiration_timestamp=NEVER_EXPIRES_TIMESTAMP,
        source="docker-postgres-e2e",
    )
    postgres_storage.add_user_profile(user_id, [profile])

    assert postgres_storage.get_request(request_id).request_id == request_id
    profiles = postgres_storage.get_user_profile(user_id)
    assert [item.profile_id for item in profiles] == [profile.profile_id]


@skip_in_precommit
def test_postgres_agent_run_round_trip(postgres_storage: PostgresStorage) -> None:
    run_id = f"pg-agent-run-{uuid.uuid4().hex[:8]}"
    request_id = f"pg-agent-request-{uuid.uuid4().hex[:8]}"

    created = postgres_storage.create_agent_run(
        AgentRunRecord(
            id=run_id,
            binding=AgentBinding(
                org_id=postgres_storage.org_id,
                extractor_kind="profile",
                user_id="pg-agent-user",
                request_id=request_id,
                agent_version="docker-postgres-e2e",
                source="docker-postgres-e2e",
                source_interaction_ids=[1, 2],
            ),
            status=AgentRunStatus.RUNNING,
            generation_request_snapshot={"request_id": request_id},
            service_config_snapshot={"window_size": 10},
        )
    )

    assert created.id == run_id
    assert created.binding.source_interaction_ids == [1, 2]
    assert created.status == AgentRunStatus.RUNNING

    updated = postgres_storage.update_agent_run_status(
        run_id,
        AgentRunStatus.FINALIZED,
        committed_output={"profiles": [{"content": "Postgres agent run works"}]},
    )

    assert updated is not None
    assert updated.status == AgentRunStatus.FINALIZED
    assert updated.committed_output == {
        "profiles": [{"content": "Postgres agent run works"}]
    }
    assert updated.finalized_at is not None

    loaded = postgres_storage.get_agent_run(run_id)
    assert loaded is not None
    assert loaded.status == AgentRunStatus.FINALIZED


@skip_in_precommit
def test_postgres_pending_tool_call_round_trip(
    postgres_storage: PostgresStorage,
) -> None:
    run_id = f"pg-agent-run-{uuid.uuid4().hex[:8]}"
    call_id = f"pg-tool-call-{uuid.uuid4().hex[:8]}"
    now = datetime(2026, 6, 16, tzinfo=UTC)
    scope = human_feedback_scope(postgres_storage.org_id)

    postgres_storage.create_agent_run(
        AgentRunRecord(
            id=run_id,
            binding=AgentBinding(
                org_id=postgres_storage.org_id,
                extractor_kind="profile",
                user_id="pg-agent-user",
                request_id=f"pg-agent-request-{uuid.uuid4().hex[:8]}",
                agent_version="docker-postgres-e2e",
                source="docker-postgres-e2e",
            ),
            status=AgentRunStatus.FINALIZED_PENDING_TOOL,
            generation_request_snapshot={"reason": "pending-tool-call-e2e"},
        )
    )
    postgres_storage.create_pending_tool_call(
        PendingToolCallRecord(
            id=call_id,
            org_id=postgres_storage.org_id,
            user_id="pg-agent-user",
            scope=scope,
            scope_hash=build_scope_hash(scope),
            tool_name="ask_human",
            dedup_key=build_pending_tool_call_dedup_key(
                tool_name="ask_human",
                question_text="Which deployment target should be used?",
            ),
            status=PendingToolCallStatus.PENDING,
            question_text="Which deployment target should be used?",
            args={"question": "Which deployment target should be used?"},
            tags=["deployment"],
            expires_at=now + timedelta(hours=1),
            cache_until=now + timedelta(minutes=5),
        )
    )
    postgres_storage.attach_run_tool_dependency(
        RunToolDependencyRecord(run_id=run_id, pending_tool_call_id=call_id)
    )

    resolved = postgres_storage.resolve_pending_tool_call(
        call_id,
        result={"answer": "AWS ECS"},
        resolved_at=now,
        valid_for_seconds=3600,
    )
    claimed = postgres_storage.claim_ready_agent_run(
        org_id=postgres_storage.org_id,
        worker_id="pg-worker",
        now=now,
    )

    assert resolved is not None
    assert resolved.status == PendingToolCallStatus.RESOLVED
    assert resolved.result == {"answer": "AWS ECS"}
    assert claimed is not None
    assert claimed.id == run_id
    assert claimed.status == AgentRunStatus.RESUMING
    assert postgres_storage.consume_run_tool_dependencies(run_id) == 1
