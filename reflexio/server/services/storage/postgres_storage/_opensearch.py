"""OpenSearch sidecar search support for native Postgres storage."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal, cast
from urllib.parse import urlparse

import boto3
from psycopg2 import sql

from reflexio.models.config_schema import EMBEDDING_DIMENSIONS, SearchMode
from reflexio.server.services.storage.error import StorageError

logger = logging.getLogger(__name__)

_CURRENT_STATUS = "__current__"
_ENTITY_INDEX_SUFFIX: dict[str, str] = {
    "profiles": "profiles",
    "interactions": "interactions",
    "user_playbooks": "user-playbooks",
    "agent_playbooks": "agent-playbooks",
}
_ENTITY_ID_FIELD: dict[str, str] = {
    "profiles": "profile_id",
    "interactions": "interaction_id",
    "user_playbooks": "user_playbook_id",
    "agent_playbooks": "agent_playbook_id",
}
_SEARCH_FIELDS = [
    "search_text^3",
    "content^2",
    "trigger^2",
    "rationale",
    "expanded_terms",
]
_SYNC_SELECT_COLUMNS: dict[str, str] = {
    "profiles": """
        profile_id, user_id, content, last_modified_timestamp,
        generated_from_request_id, profile_time_to_live, expiration_timestamp,
        custom_features, source, status, extractor_names, expanded_terms,
        source_span, notes, reader_angle, embedding::text AS embedding
    """,
    "interactions": """
        interaction_id, user_id, content, request_id, created_at, role,
        user_action, user_action_description, interacted_image_url,
        shadow_content, expert_content, tools_used, embedding::text AS embedding
    """,
    "user_playbooks": """
        user_playbook_id, user_id, playbook_name, created_at, request_id,
        agent_version, content, "trigger", rationale, blocking_issue, status,
        source, source_interaction_ids, expanded_terms, source_span, notes,
        reader_angle, embedding::text AS embedding
    """,
    "agent_playbooks": """
        agent_playbook_id, playbook_name, created_at, agent_version, content,
        "trigger", rationale, blocking_issue, playbook_status,
        playbook_metadata, expanded_terms, status, embedding::text AS embedding
    """,
}


class OpenSearchAuthMode(StrEnum):
    """Supported OpenSearch authentication modes."""

    AWS_SIGV4 = "aws_sigv4"
    NONE = "none"


@dataclass(frozen=True)
class OpenSearchConfig:
    """Runtime OpenSearch configuration resolved from environment variables."""

    endpoint: str
    auth_mode: OpenSearchAuthMode
    region: str
    service: str = "es"
    index_prefix: str = "reflexio"
    sync_on_startup: bool = True
    verify_certs: bool = True
    timeout_seconds: int = 30


def opensearch_config_from_env() -> OpenSearchConfig | None:
    """Resolve OpenSearch config from environment variables.

    Returns ``None`` when no endpoint is configured, preserving current Postgres
    behavior for tests and deployments that have not enabled OpenSearch yet.
    """
    endpoint = os.environ.get("REFLEXIO_OPENSEARCH_ENDPOINT", "").strip()
    if not endpoint:
        return None

    auth_raw = os.environ.get(
        "REFLEXIO_OPENSEARCH_AUTH", OpenSearchAuthMode.AWS_SIGV4.value
    ).strip()
    try:
        auth_mode = OpenSearchAuthMode(auth_raw)
    except ValueError as exc:
        raise StorageError(
            message=(
                "REFLEXIO_OPENSEARCH_AUTH must be one of "
                f"{[mode.value for mode in OpenSearchAuthMode]}"
            )
        ) from exc

    region = (
        os.environ.get("REFLEXIO_OPENSEARCH_REGION")
        or os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or ""
    ).strip()
    if auth_mode == OpenSearchAuthMode.AWS_SIGV4 and not region:
        raise StorageError(
            message=(
                "REFLEXIO_OPENSEARCH_REGION or AWS_REGION is required when "
                "REFLEXIO_OPENSEARCH_AUTH=aws_sigv4"
            )
        )

    verify_default = "true" if auth_mode == OpenSearchAuthMode.AWS_SIGV4 else "false"
    return OpenSearchConfig(
        endpoint=endpoint,
        auth_mode=auth_mode,
        region=region,
        service=os.environ.get("REFLEXIO_OPENSEARCH_SERVICE", "es").strip() or "es",
        index_prefix=(
            os.environ.get("REFLEXIO_OPENSEARCH_INDEX_PREFIX", "reflexio").strip()
            or "reflexio"
        ),
        sync_on_startup=_env_bool("REFLEXIO_OPENSEARCH_SYNC_ON_STARTUP", True),
        verify_certs=_env_bool("REFLEXIO_OPENSEARCH_VERIFY_CERTS", verify_default),
        timeout_seconds=_env_int("REFLEXIO_OPENSEARCH_TIMEOUT_SECONDS", 30),
    )


class PostgresOpenSearch:
    """Indexes Postgres rows and executes OpenSearch-backed search."""

    def __init__(self, storage: Any, config: OpenSearchConfig) -> None:
        self.storage = storage
        self.config = config
        self.client: Any = self._build_client()

    def _build_client(self) -> Any:
        from opensearchpy import (
            OpenSearch,
            Urllib3AWSV4SignerAuth,
            Urllib3HttpConnection,
        )

        parsed = urlparse(self.config.endpoint)
        use_ssl = parsed.scheme == "https"
        if self.config.auth_mode == OpenSearchAuthMode.NONE:
            return OpenSearch(
                hosts=[self.config.endpoint],
                use_ssl=use_ssl,
                verify_certs=self.config.verify_certs,
                ssl_show_warn=False,
                timeout=self.config.timeout_seconds,
            )

        credentials = boto3.Session().get_credentials()
        if credentials is None:
            raise StorageError(
                message="AWS credentials are required for OpenSearch SigV4 auth"
            )
        host = parsed.netloc or parsed.path
        if not host:
            raise StorageError(message="REFLEXIO_OPENSEARCH_ENDPOINT is invalid")
        port = parsed.port or (443 if use_ssl else 80)
        hostname = parsed.hostname or host
        auth = Urllib3AWSV4SignerAuth(
            credentials, self.config.region, self.config.service
        )
        return OpenSearch(
            hosts=[{"host": hostname, "port": port}],
            http_auth=auth,
            use_ssl=use_ssl,
            verify_certs=self.config.verify_certs,
            connection_class=Urllib3HttpConnection,
            pool_maxsize=20,
            timeout=self.config.timeout_seconds,
        )

    def index_name(self, entity: str) -> str:
        return f"{self.config.index_prefix}-{_ENTITY_INDEX_SUFFIX[entity]}"

    def ensure_indexes(self) -> None:
        for entity in _ENTITY_INDEX_SUFFIX:
            index = self.index_name(entity)
            if self.client.indices.exists(index=index):
                continue
            self.client.indices.create(index=index, body=_index_body())
            logger.info("Created OpenSearch index %s", index)

    def sync_all(self) -> None:
        """Backfill all searchable Postgres rows into OpenSearch."""
        self.ensure_indexes()
        for entity in _ENTITY_INDEX_SUFFIX:
            rows = self._fetch_entity_rows(entity)
            self.index_rows(entity, rows)
            logger.info("Synced %d %s rows to OpenSearch", len(rows), entity)

    def _fetch_entity_rows(self, entity: str) -> list[dict[str, Any]]:
        query = sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(_SYNC_SELECT_COLUMNS[entity]),
            self.storage._table_identifier(entity),
        )
        return cast(list[dict[str, Any]], self.storage._fetch_all(query))

    def index_rows(self, entity: str, rows: Sequence[Mapping[str, Any]]) -> None:
        if not rows:
            return
        body: list[dict[str, Any]] = []
        id_field = _ENTITY_ID_FIELD[entity]
        index = self.index_name(entity)
        for row in rows:
            doc_id = row.get(id_field)
            if doc_id in (None, ""):
                continue
            body.append({"index": {"_index": index, "_id": str(doc_id)}})
            body.append(_document(entity, row, self.storage.org_id))
        if body:
            response = self.client.bulk(body=body, refresh=True)
            if response.get("errors"):
                raise StorageError(message=f"OpenSearch bulk index failed: {response}")

    def delete_ids(self, entity: str, ids: Iterable[Any]) -> None:
        index = self.index_name(entity)
        body = [
            {"delete": {"_index": index, "_id": str(value)}}
            for value in ids
            if value not in (None, "")
        ]
        if not body:
            return
        response = self.client.bulk(body=body, refresh=True)
        if response.get("errors"):
            raise StorageError(message=f"OpenSearch bulk delete failed: {response}")

    def delete_by_filter(self, entity: str, filters: list[dict[str, Any]]) -> None:
        query: dict[str, Any] = (
            {"bool": {"filter": filters}} if filters else {"match_all": {}}
        )
        self.client.delete_by_query(
            index=self.index_name(entity),
            body={"query": query},
            conflicts="proceed",
            refresh=True,
        )

    def search_ids(
        self,
        *,
        entity: Literal[
            "profiles", "interactions", "user_playbooks", "agent_playbooks"
        ],
        query_text: str,
        query_embedding: list[float] | None,
        search_mode: SearchMode,
        top_k: int,
        threshold: float,
        filters: list[dict[str, Any]],
    ) -> list[Any]:
        if search_mode == SearchMode.VECTOR:
            return [
                hit.doc_id
                for hit in self._vector_search(
                    entity, query_embedding, top_k, threshold, filters
                )
            ]
        if search_mode == SearchMode.FTS:
            return [
                hit.doc_id
                for hit in self._text_search(entity, query_text, top_k, filters)
            ]

        overfetch = max(top_k * 5, 20)
        vector_hits = self._vector_search(
            entity, query_embedding, overfetch, threshold, filters
        )
        text_hits = self._text_search(entity, query_text, overfetch, filters)
        return _rrf_fuse([vector_hits, text_hits], top_k)

    def _text_search(
        self,
        entity: str,
        query_text: str,
        top_k: int,
        filters: list[dict[str, Any]],
    ) -> list[SearchHit]:
        body = {
            "size": top_k,
            "_source": False,
            "query": {
                "bool": {
                    "filter": filters,
                    "must": [
                        {
                            "multi_match": {
                                "query": query_text,
                                "fields": _SEARCH_FIELDS,
                                "type": "best_fields",
                            }
                        }
                    ],
                }
            },
        }
        return self._search(entity, body)

    def _vector_search(
        self,
        entity: str,
        query_embedding: list[float] | None,
        top_k: int,
        threshold: float,
        filters: list[dict[str, Any]],
    ) -> list[SearchHit]:
        if not query_embedding or len(query_embedding) != EMBEDDING_DIMENSIONS:
            return []
        base_filters = [*filters, {"exists": {"field": "embedding"}}]
        body = {
            "size": top_k,
            "_source": False,
            "min_score": 1.0 + threshold,
            "query": {
                "script_score": {
                    "query": {"bool": {"filter": base_filters}},
                    "script": {
                        "source": (
                            "cosineSimilarity(params.query_vector, "
                            "doc['embedding']) + 1.0"
                        ),
                        "params": {"query_vector": query_embedding},
                    },
                }
            },
        }
        return self._search(entity, body)

    def _search(self, entity: str, body: dict[str, Any]) -> list[SearchHit]:
        response = self.client.search(index=self.index_name(entity), body=body)
        hits = response.get("hits", {}).get("hits", [])
        return [
            SearchHit(
                doc_id=_coerce_doc_id(hit["_id"]), score=float(hit.get("_score", 0))
            )
            for hit in hits
        ]


@dataclass(frozen=True)
class SearchHit:
    doc_id: Any
    score: float


def status_filter_terms(status_filter: Sequence[Any] | None) -> list[str] | None:
    if status_filter is None:
        return None
    return [_status_value(status) for status in status_filter]


def status_term(status: Any) -> str:
    return _status_value(status)


def _index_body() -> dict[str, Any]:
    return {
        "settings": {"index": {"knn": True}},
        "mappings": {
            "properties": {
                "org_id": {"type": "keyword"},
                "profile_id": {"type": "keyword"},
                "interaction_id": {"type": "long"},
                "user_playbook_id": {"type": "long"},
                "agent_playbook_id": {"type": "long"},
                "user_id": {"type": "keyword"},
                "request_id": {"type": "keyword"},
                "agent_version": {"type": "keyword"},
                "playbook_name": {"type": "keyword"},
                "status": {"type": "keyword"},
                "playbook_status": {"type": "keyword"},
                "source": {"type": "keyword"},
                "extractor_names": {"type": "keyword"},
                "created_at": {"type": "long"},
                "last_modified_timestamp": {"type": "long"},
                "expiration_timestamp": {"type": "long"},
                "content": {"type": "text"},
                "trigger": {"type": "text"},
                "rationale": {"type": "text"},
                "expanded_terms": {"type": "text"},
                "search_text": {"type": "text"},
                "embedding": {
                    "type": "knn_vector",
                    "dimension": EMBEDDING_DIMENSIONS,
                    "method": {
                        "name": "hnsw",
                        "engine": "lucene",
                        "space_type": "cosinesimil",
                    },
                },
            }
        },
    }


def _document(entity: str, row: Mapping[str, Any], org_id: str) -> dict[str, Any]:
    id_field = _ENTITY_ID_FIELD[entity]
    doc: dict[str, Any] = {
        "org_id": org_id,
        id_field: _coerce_doc_id(row.get(id_field)),
        "user_id": row.get("user_id"),
        "request_id": row.get("request_id") or row.get("generated_from_request_id"),
        "agent_version": row.get("agent_version"),
        "playbook_name": row.get("playbook_name"),
        "status": _status_value(row.get("status")),
        "playbook_status": _enum_value(row.get("playbook_status")),
        "source": row.get("source"),
        "extractor_names": row.get("extractor_names"),
        "created_at": _timestamp(row.get("created_at")),
        "last_modified_timestamp": _timestamp(row.get("last_modified_timestamp")),
        "expiration_timestamp": _timestamp(row.get("expiration_timestamp")),
        "content": row.get("content") or "",
        "trigger": row.get("trigger") or "",
        "rationale": row.get("rationale") or "",
        "expanded_terms": row.get("expanded_terms") or "",
    }
    text_parts = [
        doc["content"],
        doc["trigger"],
        doc["rationale"],
        doc["expanded_terms"],
        row.get("user_action_description") or "",
        row.get("shadow_content") or "",
        row.get("expert_content") or "",
        str(row.get("custom_features") or ""),
        str(row.get("blocking_issue") or ""),
        str(row.get("playbook_metadata") or ""),
        str(row.get("notes") or ""),
        str(row.get("reader_angle") or ""),
    ]
    doc["search_text"] = "\n".join(part for part in text_parts if part)
    embedding = _embedding(row.get("embedding"))
    if embedding is not None:
        doc["embedding"] = embedding
    return {k: v for k, v in doc.items() if v is not None}


def _embedding(value: Any) -> list[float] | None:
    if isinstance(value, list):
        vector = [float(v) for v in value]
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped == "[]":
            return None
        vector = [float(part) for part in stripped.strip("[]").split(",") if part]
    else:
        return None
    return vector if len(vector) == EMBEDDING_DIMENSIONS else None


def _timestamp(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, str):
        if value.isdigit():
            return int(value)
        try:
            return int(datetime.fromisoformat(value).timestamp())
        except ValueError:
            return None
    return None


def _status_value(value: Any) -> str:
    raw = _enum_value(value)
    return _CURRENT_STATUS if raw in (None, "") else str(raw)


def _enum_value(value: Any) -> Any:
    return value.value if hasattr(value, "value") else value


def _coerce_doc_id(value: Any) -> Any:
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return value


def _rrf_fuse(
    hit_lists: list[list[SearchHit]], top_k: int, rrf_k: int = 60
) -> list[Any]:
    scores: dict[Any, float] = {}
    first_seen: dict[Any, int] = {}
    ordinal = 0
    for hits in hit_lists:
        for rank, hit in enumerate(hits, start=1):
            if hit.doc_id not in first_seen:
                first_seen[hit.doc_id] = ordinal
                ordinal += 1
            scores[hit.doc_id] = scores.get(hit.doc_id, 0.0) + (1.0 / (rrf_k + rank))
    ordered = sorted(scores, key=lambda doc_id: (-scores[doc_id], first_seen[doc_id]))
    return ordered[:top_k]


def _env_bool(name: str, default: bool | str) -> bool:
    raw_default = str(default).lower()
    raw = os.environ.get(name, raw_default).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw.isdigit() else default
