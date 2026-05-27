from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from reflexio.models.config_schema import (
    EMBEDDING_DIMENSIONS,
    PostgresSearchBackend,
    SearchMode,
)
from reflexio.server.services.storage.error import StorageError
from reflexio.server.services.storage.postgres_storage._opensearch import (
    OpenSearchAuthMode,
    OpenSearchConfig,
    PostgresOpenSearch,
    opensearch_config_from_env,
)


def test_opensearch_config_disabled_without_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("REFLEXIO_OPENSEARCH_ENDPOINT", raising=False)

    assert opensearch_config_from_env() is None


def test_opensearch_config_local_none_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REFLEXIO_OPENSEARCH_ENDPOINT", "http://localhost:19200")
    monkeypatch.setenv("REFLEXIO_OPENSEARCH_AUTH", "none")
    monkeypatch.delenv("REFLEXIO_OPENSEARCH_REGION", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)

    config = opensearch_config_from_env()

    assert config is not None
    assert config.auth_mode == OpenSearchAuthMode.NONE
    assert config.endpoint == "http://localhost:19200"
    assert config.verify_certs is False


def test_opensearch_config_sigv4_requires_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REFLEXIO_OPENSEARCH_ENDPOINT", "https://search.example.com")
    monkeypatch.setenv("REFLEXIO_OPENSEARCH_AUTH", "aws_sigv4")
    monkeypatch.delenv("REFLEXIO_OPENSEARCH_REGION", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)

    with pytest.raises(StorageError, match="REGION"):
        opensearch_config_from_env()


def test_opensearch_config_sigv4_uses_aws_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REFLEXIO_OPENSEARCH_ENDPOINT", "https://search.example.com")
    monkeypatch.setenv("REFLEXIO_OPENSEARCH_AUTH", "aws_sigv4")
    monkeypatch.setenv("AWS_REGION", "us-west-2")

    config = opensearch_config_from_env()

    assert config is not None
    assert config.auth_mode == OpenSearchAuthMode.AWS_SIGV4
    assert config.region == "us-west-2"
    assert config.service == "es"


def test_postgres_search_backend_env_defaults_to_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from reflexio.server.services.configurator.postgres_env import (
        postgres_search_backend_from_env,
    )

    monkeypatch.delenv("REFLEXIO_POSTGRES_SEARCH_BACKEND", raising=False)

    assert postgres_search_backend_from_env() == PostgresSearchBackend.POSTGRES


def test_postgres_search_backend_env_accepts_opensearch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from reflexio.server.services.configurator.postgres_env import (
        postgres_search_backend_from_env,
    )

    monkeypatch.setenv("REFLEXIO_POSTGRES_SEARCH_BACKEND", "opensearch")

    assert postgres_search_backend_from_env() == PostgresSearchBackend.OPENSEARCH


def test_opensearch_index_upsert_delete_and_search() -> None:
    fake_client = FakeOpenSearchClient()
    searcher = object.__new__(PostgresOpenSearch)
    searcher.storage = StorageStub()
    searcher.config = OpenSearchConfig(
        endpoint="http://localhost:19200",
        auth_mode=OpenSearchAuthMode.NONE,
        region="",
        index_prefix="test-reflexio",
    )
    searcher.client = fake_client

    searcher.ensure_indexes()
    searcher.index_rows(
        "profiles",
        [
            {
                "profile_id": "p1",
                "user_id": "u1",
                "content": "Prefers local OpenSearch verification.",
                "status": None,
                "expiration_timestamp": 4_102_444_800,
                "embedding": [0.1] * EMBEDDING_DIMENSIONS,
            }
        ],
    )
    ids = searcher.search_ids(
        entity="profiles",
        query_text="OpenSearch verification",
        query_embedding=[0.1] * EMBEDDING_DIMENSIONS,
        search_mode=SearchMode.HYBRID,
        top_k=5,
        threshold=0.1,
        filters=[{"term": {"user_id": "u1"}}],
    )
    searcher.delete_ids("profiles", ids)

    assert "test-reflexio-profiles" in fake_client.indices.created
    assert fake_client.bulk_actions[0]["index"]["_id"] == "p1"
    assert ids == ["p1"]
    assert fake_client.bulk_actions[-1]["delete"]["_id"] == "p1"


@dataclass
class StorageStub:
    org_id: str = "test-org"


class FakeIndices:
    def __init__(self) -> None:
        self.created: dict[str, dict[str, Any]] = {}

    def exists(self, index: str) -> bool:
        return index in self.created

    def create(self, index: str, body: dict[str, Any]) -> None:
        self.created[index] = body


class FakeOpenSearchClient:
    def __init__(self) -> None:
        self.indices = FakeIndices()
        self.bulk_actions: list[dict[str, Any]] = []

    def bulk(self, body: list[dict[str, Any]], refresh: bool) -> dict[str, Any]:  # noqa: ARG002
        self.bulk_actions.extend(body)
        return {"errors": False}

    def search(self, index: str, body: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        return {
            "hits": {
                "hits": [
                    {"_id": "p1", "_score": 2.0},
                ]
            }
        }

    def delete_by_query(
        self,
        index: str,  # noqa: ARG002
        body: dict[str, Any],  # noqa: ARG002
        conflicts: str,  # noqa: ARG002
        refresh: bool,  # noqa: ARG002
    ) -> None:
        return None
