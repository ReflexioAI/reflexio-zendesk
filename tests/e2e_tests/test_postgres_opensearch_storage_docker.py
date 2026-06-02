"""Live Postgres + local OpenSearch storage smoke test.

Skipped unless both a Postgres test URL and REFLEXIO_OPENSEARCH_ENDPOINT are
configured. For local verification, run OpenSearch from docker-compose.yml with
REFLEXIO_OPENSEARCH_AUTH=none.
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Generator
from unittest.mock import patch

import psycopg2
import pytest
from psycopg2 import sql

from reflexio.models.api_schema.retriever_schema import SearchUserProfileRequest
from reflexio.models.api_schema.service_schemas import (
    NEVER_EXPIRES_TIMESTAMP,
    Interaction,
    ProfileTimeToLive,
    Request,
    UserProfile,
)
from reflexio.models.config_schema import PostgresSearchBackend, StorageConfigPostgres
from reflexio.server.services.storage.postgres_storage import PostgresStorage
from tests.server.test_utils import skip_in_precommit


def _postgres_db_url() -> str:
    return (
        os.environ.get("POSTGRES_TEST_DB_URL")
        or os.environ.get("POSTGRES_DB_URL")
        or os.environ.get("REFLEXIO_POSTGRES_DB_URL")
        or ""
    )


@pytest.fixture
def postgres_opensearch_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[PostgresStorage]:
    db_url = _postgres_db_url()
    if not db_url:
        pytest.skip(
            "Set POSTGRES_TEST_DB_URL, POSTGRES_DB_URL, or REFLEXIO_POSTGRES_DB_URL"
        )
    if not os.environ.get("REFLEXIO_OPENSEARCH_ENDPOINT"):
        pytest.skip("Set REFLEXIO_OPENSEARCH_ENDPOINT for OpenSearch E2E")

    schema = f"e2e_os_{uuid.uuid4().hex[:12]}"
    index_prefix = f"reflexio-e2e-{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("REFLEXIO_OPENSEARCH_INDEX_PREFIX", index_prefix)
    monkeypatch.setenv(
        "REFLEXIO_OPENSEARCH_AUTH", os.environ.get("REFLEXIO_OPENSEARCH_AUTH", "none")
    )
    monkeypatch.setenv("REFLEXIO_OPENSEARCH_SYNC_ON_STARTUP", "true")

    storage = PostgresStorage(
        org_id=f"postgres-opensearch-e2e-{schema}",
        config=StorageConfigPostgres(
            db_url=db_url,
            schema=schema,
            pool_size=2,
            search_backend=PostgresSearchBackend.OPENSEARCH,
        ),
    )
    try:
        with patch.object(storage, "_get_embedding", return_value=[0.1] * 512):
            yield storage
    finally:
        if storage._opensearch:
            for entity in (
                "profiles",
                "interactions",
                "user_playbooks",
                "agent_playbooks",
            ):
                index = storage._opensearch.index_name(entity)
                if storage._opensearch.client.indices.exists(index=index):
                    storage._opensearch.client.indices.delete(index=index)
        storage.close()
        with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


@skip_in_precommit
def test_postgres_opensearch_profile_search_round_trip(
    postgres_opensearch_storage: PostgresStorage,
) -> None:
    run_id = uuid.uuid4().hex[:8]
    user_id = f"os-user-{run_id}"
    request_id = f"os-request-{run_id}"
    now = int(time.time())

    postgres_opensearch_storage.add_request(
        Request(
            request_id=request_id,
            user_id=user_id,
            created_at=now,
            source="docker-opensearch-e2e",
            agent_version="codex",
            session_id=f"session-{run_id}",
        )
    )
    profile = UserProfile(
        profile_id=f"os-profile-{run_id}",
        user_id=user_id,
        content="Always verify Reflexio search with local OpenSearch.",
        last_modified_timestamp=now,
        generated_from_request_id=request_id,
        profile_time_to_live=ProfileTimeToLive.INFINITY,
        expiration_timestamp=NEVER_EXPIRES_TIMESTAMP,
        source="docker-opensearch-e2e",
    )

    postgres_opensearch_storage.add_user_profile(user_id, [profile])

    results = postgres_opensearch_storage.search_user_profile(
        SearchUserProfileRequest(
            user_id=user_id,
            query="local OpenSearch verification",
            top_k=5,
            threshold=0.1,
        )
    )

    assert [item.profile_id for item in results] == [profile.profile_id]


@skip_in_precommit
def test_postgres_opensearch_delete_all_requests_clears_interaction_index(
    postgres_opensearch_storage: PostgresStorage,
) -> None:
    run_id = uuid.uuid4().hex[:8]
    user_id = f"os-delete-user-{run_id}"
    request_id = f"os-delete-request-{run_id}"
    now = int(time.time())

    postgres_opensearch_storage.add_request(
        Request(
            request_id=request_id,
            user_id=user_id,
            created_at=now,
            source="docker-opensearch-delete-e2e",
            agent_version="codex",
            session_id=f"session-{run_id}",
        )
    )
    postgres_opensearch_storage.add_user_interaction(
        user_id,
        Interaction(
            interaction_id=101,
            user_id=user_id,
            request_id=request_id,
            created_at=now,
            role="User",
            content="Delete all requests should also clear OpenSearch interactions.",
        ),
    )

    assert postgres_opensearch_storage._opensearch is not None
    index = postgres_opensearch_storage._opensearch.index_name("interactions")
    assert (
        postgres_opensearch_storage._opensearch.client.count(index=index)["count"] == 1
    )

    postgres_opensearch_storage.delete_all_requests()

    assert postgres_opensearch_storage.get_user_interaction(user_id) == []
    assert (
        postgres_opensearch_storage._opensearch.client.count(index=index)["count"] == 0
    )
