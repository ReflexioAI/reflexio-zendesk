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
    GetSessionOutcomesRequest,
    Interaction,
    LineageContext,
    ProfileTimeToLive,
    Request,
    RetrievedLearning,
    RetrievedLearningEvaluationResult,
    SessionOutcomeKind,
    SetSessionOutcomeRequest,
    UserPlaybook,
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
from reflexio.server.services.storage.storage_base.retrieved_learning_state import (
    session_fingerprint,
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
    assert (
        postgres_storage.get_agent_run_finalization_receipt(
            run_id=run_id,
            entity_type="profile",
        )
        is None
    )
    assert postgres_storage.save_agent_run_finalization_receipt(
        run_id=run_id,
        entity_type="profile",
        learning_ids=["profile-1", "profile-2"],
    )
    assert not postgres_storage.save_agent_run_finalization_receipt(
        run_id=run_id,
        entity_type="profile",
        learning_ids=["profile-1", "profile-2"],
    )
    assert postgres_storage.get_agent_run_finalization_receipt(
        run_id=run_id,
        entity_type="profile",
    ) == ["profile-1", "profile-2"]

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


@skip_in_precommit
def test_postgres_lineage_round_trip(
    postgres_storage: PostgresStorage,
) -> None:
    run_id = uuid.uuid4().hex[:8]
    request_id = f"pg-lineage-request-{run_id}"
    user_id = f"pg-lineage-user-{run_id}"

    first = UserPlaybook(
        user_id=user_id,
        request_id=request_id,
        agent_version="codex",
        playbook_name="postgres-lineage",
        content="Old Postgres storage retrieval guidance.",
    )
    successor = UserPlaybook(
        user_id=user_id,
        request_id=request_id,
        agent_version="codex",
        playbook_name="postgres-lineage",
        content="New Postgres storage retrieval guidance.",
    )
    postgres_storage.save_user_playbooks([first, successor])

    assert first.user_playbook_id
    assert successor.user_playbook_id
    assert postgres_storage.supersede_record(
        entity_type="user_playbook",
        incumbent_id=str(first.user_playbook_id),
        successor_id=str(successor.user_playbook_id),
        context=LineageContext(
            op_kind="revise",
            actor="postgres-e2e",
            request_id=request_id,
            reason="verify postgres lineage",
        ),
    )

    events = postgres_storage.get_lineage_events(
        entity_type="user_playbook",
        entity_id=str(successor.user_playbook_id),
        request_id=request_id,
    )
    assert [(event.op, event.source_ids) for event in events] == [
        ("create", []),
        ("revise", [str(first.user_playbook_id)]),
    ]


@skip_in_precommit
def test_postgres_incremental_playbook_aggregation_contract(
    postgres_storage: PostgresStorage,
) -> None:
    run_id = uuid.uuid4().hex[:8]
    agent_version = f"pg-aggregation-{run_id}"
    playbooks = [
        UserPlaybook(
            user_id=f"pg-aggregation-user-{run_id}",
            request_id=f"pg-aggregation-request-{run_id}-{index}",
            agent_version=agent_version,
            playbook_name="postgres-aggregation",
            content=f"Postgres aggregation evidence {index}",
            trigger=f"When Postgres aggregation case {index} occurs",
        )
        for index in range(2)
    ]
    postgres_storage.save_user_playbooks(playbooks)

    assert postgres_storage.supports_incremental_playbook_aggregation is True
    postgres_storage.schedule_playbook_aggregation(agent_version)
    claim = postgres_storage.claim_due_playbook_aggregation(
        owner="postgres-aggregation-e2e",
        lease_seconds=60,
        agent_version=agent_version,
    )
    assert claim is not None
    assert postgres_storage.validate_playbook_aggregation_claim(claim)

    staged = postgres_storage.stage_playbook_aggregation_intake(
        agent_version,
        limit=100,
    )
    expected_ids: list[int] = []
    for playbook in playbooks:
        assert playbook.user_playbook_id is not None
        expected_ids.append(playbook.user_playbook_id)
    assert staged == sorted(expected_ids, reverse=True)
    backlog = postgres_storage.get_playbook_aggregation_backlog(agent_version)
    assert backlog.residual == 2
    assert backlog.pending
    assert postgres_storage.finish_playbook_aggregation_claim(
        claim,
        success=True,
        retry_after_seconds=60,
        backlog_retry_after_seconds=0,
        min_interval_seconds=3_600,
        backlog=backlog,
    )


@skip_in_precommit
def test_postgres_upstream_storage_contract_round_trip(
    postgres_storage: PostgresStorage,
) -> None:
    run_id = uuid.uuid4().hex[:8]
    user_id = f"pg-contract-user-{run_id}"
    request_id = f"pg-contract-request-{run_id}"
    session_id = f"pg-contract-session-{run_id}"
    now = int(time.time())
    postgres_storage.add_request(
        Request(
            request_id=request_id,
            user_id=user_id,
            created_at=now,
            source="docker-postgres-contract-e2e",
            agent_version="codex",
            session_id=session_id,
        )
    )

    context = postgres_storage.get_session_outcome_context(session_id)
    outcome = postgres_storage.record_session_outcome(
        SetSessionOutcomeRequest(
            session_id=session_id,
            outcome=SessionOutcomeKind.SUCCESS,
            occurred_at=now + 1,
            label="resolved",
            metadata={"path": "postgres"},
        ),
        created_at=now + 1,
        expected_context=context,
    )
    assert outcome.recorded
    assert outcome.outcome_id
    assert outcome.outcome_revision == 1
    assert outcome.outcome_contract_digest
    assert outcome.finalized_trajectory_digest
    exact_retry = postgres_storage.record_session_outcome(
        SetSessionOutcomeRequest(
            session_id=session_id,
            outcome=SessionOutcomeKind.SUCCESS,
            occurred_at=now + 1,
            label="resolved",
            metadata={"path": "postgres"},
        ),
        created_at=now + 2,
        expected_context=context,
    )
    assert not exact_retry.recorded
    assert exact_retry.reason is None
    assert exact_retry.outcome_id == outcome.outcome_id
    stored_outcomes = postgres_storage.get_session_outcomes(
        GetSessionOutcomesRequest(session_ids=[session_id])
    )
    assert [(item.session_id, item.outcome) for item in stored_outcomes] == [
        (session_id, SessionOutcomeKind.SUCCESS)
    ]

    first_job_id = postgres_storage.enqueue_learning_job(
        org_id=postgres_storage.org_id,
        user_id=user_id,
        request_id=request_id,
        covers_through=float(now),
    )
    second_job_id = postgres_storage.enqueue_learning_job(
        org_id=postgres_storage.org_id,
        user_id=user_id,
        request_id=request_id,
        covers_through=float(now + 1),
    )
    assert second_job_id == first_job_id
    claimed = postgres_storage.claim_learning_jobs(
        claimed_by="postgres-e2e", limit=1, lease_seconds=60
    )
    assert [job.job_id for job in claimed] == [first_job_id]
    assert claimed[0].claim_token
    assert postgres_storage.heartbeat_learning_job(
        job_id=first_job_id,
        claim_token=claimed[0].claim_token,
        lease_seconds=60,
    )
    assert (
        postgres_storage.complete_learning_job(
            job_id=first_job_id, claim_token="stale-fence"
        )
        == 0
    )
    assert (
        postgres_storage.complete_learning_job(
            job_id=first_job_id, claim_token=claimed[0].claim_token
        )
        == 1
    )

    playbook = UserPlaybook(
        user_id=user_id,
        request_id=request_id,
        agent_version="codex",
        playbook_name="postgres-contract",
        content="Retrieve the Postgres contract learning.",
        trigger="When Postgres storage contracts are verified",
    )
    postgres_storage.save_user_playbooks([playbook])
    assert playbook.user_playbook_id
    postgres_storage.add_user_interaction(
        user_id,
        Interaction(
            interaction_id=101,
            user_id=user_id,
            request_id=request_id,
            created_at=now + 2,
            role="Assistant",
            content="Applied the Postgres contract learning.",
            retrieved_learnings=[
                RetrievedLearning(
                    kind="user_playbook",
                    learning_id=str(playbook.user_playbook_id),
                )
            ],
        ),
    )
    snapshot = postgres_storage.load_bounded_retrieved_learning_snapshot(
        user_id, session_id
    )
    fingerprint = session_fingerprint(snapshot)
    generation = postgres_storage.begin_retrieved_learning_evaluation_run(
        user_id, session_id
    )
    commit = postgres_storage.replace_retrieved_learning_evaluation_results(
        user_id,
        session_id,
        generation,
        fingerprint,
        "complete",
        {"source": "postgres-e2e"},
        [
            RetrievedLearningEvaluationResult(
                user_id=user_id,
                session_id=session_id,
                agent_version="codex",
                interaction_id=101,
                interaction_created_at=now + 2,
                kind="user_playbook",
                learning_id=str(playbook.user_playbook_id),
                is_relevant=True,
                relevance_reason="Used by the response",
                created_at=now + 3,
            )
        ],
    )
    assert (commit.disposition, commit.status, commit.committed_count) == (
        "applied",
        "complete",
        1,
    )
    stored_evaluations = postgres_storage.get_retrieved_learning_evaluation_results(
        user_id=user_id, session_id=session_id
    )
    assert [(item.kind, item.learning_id) for item in stored_evaluations] == [
        ("user_playbook", str(playbook.user_playbook_id))
    ]

    purge_id = "purge_contract_e2e"
    subject_ref = postgres_storage._subject_ref_for_user_id(user_id)
    postgres_storage.begin_purge_operation(
        purge_id=purge_id,
        idempotency_key=purge_id,
        operation_type="user_erasure",
        scope_type="user",
        subject_ref=subject_ref,
        request_ref="reqref_v1_contract_e2e",
        authoritative_user_id=user_id,
    )
    execution_claim = postgres_storage.claim_purge_operation_execution(
        purge_id,
        lease_owner="postgres-contract-e2e",
        lease_ttl_seconds=60,
    )
    assert execution_claim is not None
    postgres_storage.begin_subject_erasure_barrier(
        subject_ref,
        purge_id,
        execution_claim=execution_claim,
    )
    postgres_storage.prepare_governance_erase_targets(
        purge_id,
        user_id,
        execution_claim=execution_claim,
        owned_user_playbook_ids={int(playbook.user_playbook_id)},
    )
    deleted = postgres_storage.apply_governance_user_data_delete(
        purge_id,
        user_id,
        execution_claim=execution_claim,
    )

    assert deleted["session_outcomes"] == 1
    assert deleted["retrieved_learning_evaluation_results"] == 1
    assert deleted["evaluation_operation_states"] == 1
    assert (
        postgres_storage.get_session_outcomes(
            GetSessionOutcomesRequest(session_ids=[session_id])
        )
        == []
    )
    assert (
        postgres_storage.get_retrieved_learning_evaluation_results(
            user_id=user_id, session_id=session_id
        )
        == []
    )
    delete_targets = postgres_storage.list_purge_targets(purge_id, phase="delete")
    assert len(delete_targets) == 12
    assert all(target.status == "complete" for target in delete_targets)
