"""Playbook CRUD + search methods for Supabase storage.

Update helpers in this module treat ``None`` as "leave the field unchanged";
use an explicit sentinel if a caller needs to clear nullable columns.
"""

import json
import logging
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any, cast

from psycopg2 import sql

from reflexio.models.api_schema.common import BlockingIssue
from reflexio.models.api_schema.retriever_schema import (
    SearchAgentPlaybookRequest,
    SearchUserPlaybookRequest,
)
from reflexio.models.api_schema.service_schemas import (
    AgentPlaybook,
    AgentPlaybookSourceWindow,
    AgentSuccessEvaluationResult,
    PlaybookOptimizationCandidate,
    PlaybookOptimizationEvaluation,
    PlaybookOptimizationEvent,
    PlaybookOptimizationJob,
    PlaybookStatus,
    RegularVsShadow,
    Status,
    UserPlaybook,
)
from reflexio.models.config_schema import SearchOptions
from reflexio.server.services.storage.postgres_storage._opensearch import (
    status_filter_terms,
)
from reflexio.server.services.storage.postgres_storage._playbook_converters import (
    agent_playbook_to_data,
    agent_success_evaluation_result_to_data,
    user_playbook_to_data,
)
from reflexio.server.services.storage.storage_base import matches_status_filter
from reflexio.server.usage_metrics import record_usage_event

from ._base import (
    _AGENT_PLAYBOOK_COLUMNS,
    _EVAL_RESULT_COLUMNS,
    _USER_PLAYBOOK_COLUMNS,
    _USER_PLAYBOOK_COLUMNS_WITH_EMBEDDING,
    PostgresStorageBase,
    _apply_status_filter_to_query,
    _build_status_or_condition,
    _rows,
    _timestamp_to_iso,
)
from ._protocols import SchemaScopedClient

logger = logging.getLogger(__name__)

handle_exceptions = PostgresStorageBase.handle_exceptions


def _json_data(text: str, default: Any | None = None) -> Any:
    fallback = {} if default is None else default
    try:
        return json.loads(text) if text else fallback
    except json.JSONDecodeError:
        return fallback


def _json_text(value: Any) -> str:
    return json.dumps({} if value is None else value, ensure_ascii=False)


def _int_list(value: Any) -> list[int]:
    if value is None:
        return []
    if isinstance(value, str):
        value = _json_data(value, [])
    if not isinstance(value, list):
        return []
    return [int(item) for item in value]


def _parse_user_playbook_embedding(raw: Any) -> list[float]:
    """Parse a Postgres pgvector string (e.g. ``"[0.1,0.2,...]"``) into a float list.

    Args:
        raw (Any): Raw embedding value from the row, typically a string.

    Returns:
        list[float]: Parsed embedding. Returns ``[]`` for falsy values,
            non-string inputs, or strings that contain no numeric parts
            (including the literal ``"[]"``). Tokens that fail to parse
            as float are skipped rather than raising — the caller gets
            whatever well-formed values were present.
    """
    if not raw or not isinstance(raw, str):
        return []
    # Trim outer whitespace before bracket-stripping so padded inputs
    # like "  [0.1, 0.2]  " don't end up with bracket residue on each
    # token (which would then fail float()).
    inner = raw.strip().strip("[]")
    out: list[float] = []
    for p in inner.split(","):
        if not p.strip():
            continue
        try:
            out.append(float(p))
        except (ValueError, TypeError):
            continue
    return out


class PlaybookMixin(SchemaScopedClient):
    # Type hints for instance attributes/methods provided by PostgresStorageBase via MRO
    client: Any
    org_id: str
    _get_embedding: Any
    _should_expand_documents: Any
    _expand_document: Any
    _parse_datetime_to_timestamp: Any
    _fetch_all: Any
    _table_identifier: Any
    search_mode: Any
    vector_weight: float
    fts_weight: float
    _opensearch: Any

    def _record_playbook_event(
        self,
        *,
        event_name: str,
        outcome: str,
        entity_type: str,
        entity_id: str | None,
        **extra: Any,
    ) -> None:
        record_usage_event(
            org_id=self.org_id,
            event_category="entity_change",
            pipeline="playbook",
            event_name=event_name,
            outcome=outcome,
            entity_type=entity_type,
            entity_id=entity_id,
            **extra,
        )

    def _row_to_user_playbook(
        self, item: dict[str, Any], *, include_embedding: bool = False
    ) -> UserPlaybook:
        """Build a ``UserPlaybook`` from a Supabase ``user_playbooks`` row.

        Args:
            item (dict[str, Any]): Row dict as returned by the Supabase client.
            include_embedding (bool): Whether to parse the row's ``embedding``
                column. When ``False`` (default), the resulting model carries
                an empty embedding list — matching the behavior of callers
                that don't need the vector.

        Returns:
            UserPlaybook: Hydrated playbook model.
        """
        return UserPlaybook(
            user_playbook_id=int(item["user_playbook_id"]),
            user_id=item.get("user_id"),
            playbook_name=item["playbook_name"],
            created_at=self._parse_datetime_to_timestamp(item["created_at"]),
            request_id=item["request_id"],
            agent_version=item["agent_version"],
            content=item["content"],
            trigger=item.get("trigger"),
            rationale=item.get("rationale"),
            blocking_issue=BlockingIssue(**item["blocking_issue"])
            if item.get("blocking_issue")
            else None,
            status=Status(item["status"]) if item.get("status") else None,
            source=item.get("source"),
            source_interaction_ids=item.get("source_interaction_ids") or [],
            expanded_terms=item.get("expanded_terms"),
            tags=item.get("tags"),
            embedding=_parse_user_playbook_embedding(item.get("embedding"))
            if include_embedding
            else [],
            source_span=item.get("source_span"),
            notes=item.get("notes"),
            reader_angle=item.get("reader_angle"),
            merged_into=item.get("merged_into"),
            superseded_by=item.get("superseded_by"),
        )

    def _row_to_agent_playbook(self, item: dict[str, Any]) -> AgentPlaybook:
        return AgentPlaybook(
            agent_playbook_id=item["agent_playbook_id"],
            playbook_name=item["playbook_name"],
            created_at=self._parse_datetime_to_timestamp(item["created_at"]),
            agent_version=item["agent_version"],
            content=item["content"],
            trigger=item.get("trigger"),
            rationale=item.get("rationale"),
            blocking_issue=BlockingIssue(**item["blocking_issue"])
            if item.get("blocking_issue")
            else None,
            playbook_status=item["playbook_status"],
            playbook_metadata=item.get("playbook_metadata") or "",
            expanded_terms=item.get("expanded_terms"),
            tags=item.get("tags"),
            embedding=[],
            status=Status(item["status"]) if item.get("status") else None,
            merged_into=item.get("merged_into"),
            superseded_by=item.get("superseded_by"),
        )

    # ==============================
    # User Playbook methods
    # ==============================

    @handle_exceptions
    def save_user_playbooks(self, user_playbooks: list[UserPlaybook]) -> None:
        for user_playbook in user_playbooks:
            is_new = not user_playbook.user_playbook_id
            embedding_text = user_playbook.trigger or user_playbook.content
            if embedding_text:
                if self._should_expand_documents():
                    with ThreadPoolExecutor(max_workers=2) as executor:
                        emb_future = executor.submit(
                            self._get_embedding, embedding_text
                        )
                        exp_future = executor.submit(
                            self._expand_document, embedding_text
                        )
                        user_playbook.embedding = emb_future.result(timeout=15)
                        user_playbook.expanded_terms = exp_future.result(timeout=15)
                else:
                    user_playbook.embedding = self._get_embedding(embedding_text)
            response = (
                self._table("user_playbooks")
                .upsert(user_playbook_to_data(user_playbook))
                .execute()
            )
            if response.data and isinstance(response.data, list):
                row = cast(dict[str, Any], response.data[0])
                user_playbook.user_playbook_id = row.get(
                    "user_playbook_id", user_playbook.user_playbook_id
                )
            if self._opensearch:
                self._opensearch.index_rows("user_playbooks", _rows(response))
            self._record_playbook_event(
                event_name=(
                    "user_playbook_created" if is_new else "user_playbook_updated"
                ),
                outcome="created" if is_new else "updated",
                entity_type="user_playbook",
                entity_id=(str(user_playbook.user_playbook_id) if not is_new else None),
                user_id=user_playbook.user_id,
                request_id=user_playbook.request_id,
                playbook_name=user_playbook.playbook_name,
                source=user_playbook.source,
                agent_version=user_playbook.agent_version,
            )

    @handle_exceptions
    def get_user_playbooks(
        self,
        limit: int = 100,
        user_id: str | None = None,
        playbook_name: str | None = None,
        agent_version: str | None = None,
        status_filter: list[Status | None] | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        include_embedding: bool = False,
        tags: list[str] | None = None,
        offset: int = 0,
    ) -> list[UserPlaybook]:
        """
        Get user playbooks from storage.

        Args:
            limit (int): Maximum number of user playbooks to return
            user_id (str, optional): The user ID to filter by. If None, returns playbooks for all users.
            playbook_name (str, optional): The playbook name to filter by. If None, returns all user playbooks.
            agent_version (str, optional): The agent version to filter by. If None, returns all agent versions.
            status_filter (list[Optional[Status]], optional): List of status values to filter by.
                Can include None (current), Status.PENDING (from rerun), Status.ARCHIVED (old).
                If None, returns playbooks with all statuses.
            start_time (int, optional): Unix timestamp. Only return playbooks created at or after this time.
            end_time (int, optional): Unix timestamp. Only return playbooks created at or before this time.
            include_embedding (bool): If True, fetch and parse embedding vectors. Defaults to False.

        Returns:
            list[UserPlaybook]: List of user playbook objects
        """
        columns = (
            _USER_PLAYBOOK_COLUMNS_WITH_EMBEDDING
            if include_embedding
            else _USER_PLAYBOOK_COLUMNS
        )
        query = (
            self._table("user_playbooks")
            .select(columns)
            .order("created_at", desc=True)
            .limit(limit)
            .offset(offset)
        )

        # Add user_id filter if specified
        if user_id is not None:
            query = query.eq("user_id", user_id)

        # Add playbook_name filter if specified (skip if None or empty string)
        if playbook_name:
            query = query.eq("playbook_name", playbook_name)

        # Add agent_version filter if specified
        if agent_version is not None:
            query = query.eq("agent_version", agent_version)

        # Add time range filters if specified
        if start_time is not None:
            start_time_iso = datetime.fromtimestamp(start_time, tz=UTC).isoformat()
            query = query.gte("created_at", start_time_iso)
        if end_time is not None:
            end_time_iso = datetime.fromtimestamp(end_time, tz=UTC).isoformat()
            query = query.lte("created_at", end_time_iso)

        # Add status filter if specified
        if status_filter is not None:
            query = _apply_status_filter_to_query(query, status_filter)
        if tags:
            query = query.contains("tags", tags)

        response = query.execute()
        return [
            self._row_to_user_playbook(item, include_embedding=include_embedding)
            for item in _rows(response)
        ]

    @handle_exceptions
    def count_user_playbooks(
        self,
        user_id: str | None = None,
        playbook_name: str | None = None,
        min_user_playbook_id: int | None = None,
        agent_version: str | None = None,
        status_filter: list[Status | None] | None = None,
    ) -> int:
        """
        Count user playbooks in storage efficiently using SQL COUNT.

        Args:
            user_id (str, optional): The user ID to filter by. If None, counts playbooks for all users.
            playbook_name (str, optional): The playbook name to filter by. If None, counts all user playbooks.
            min_user_playbook_id (int, optional): Only count playbooks with user_playbook_id greater than this value.
            agent_version (str, optional): The agent version to filter by. If None, counts all agent versions.
            status_filter (list[Optional[Status]], optional): List of status values to filter by.
                Can include None (current), Status.PENDING (from rerun), Status.ARCHIVED (old).
                If None, returns playbooks with all statuses.

        Returns:
            int: Count of user playbooks matching the filters
        """
        query = self._table("user_playbooks").select(
            "user_playbook_id",
            count="exact",  # type: ignore[reportArgumentType]
        )

        # Add user_id filter if specified
        if user_id is not None:
            query = query.eq("user_id", user_id)

        # Add playbook_name filter if specified (skip if None or empty string)
        if playbook_name:
            query = query.eq("playbook_name", playbook_name)

        # Add min_user_playbook_id filter if specified
        if min_user_playbook_id is not None:
            query = query.gt("user_playbook_id", min_user_playbook_id)

        # Add agent_version filter if specified
        if agent_version is not None:
            query = query.eq("agent_version", agent_version)

        # Add status filter if specified
        if status_filter is not None:
            query = _apply_status_filter_to_query(query, status_filter)

        response = query.execute()
        return response.count if response.count is not None else 0

    @handle_exceptions
    def count_user_playbooks_by_session(self, session_id: str) -> int:
        """
        Count user playbooks linked to a session via request_id -> requests.session_id.

        Args:
            session_id (str): The session ID to count user playbooks for

        Returns:
            int: Count of user playbooks linked to the session
        """
        # First get all request_ids for this session
        requests_response = (
            self._table("requests")
            .select("request_id")
            .eq("session_id", session_id)
            .execute()
        )

        requests_data = _rows(requests_response)
        if not requests_data:
            return 0

        request_ids = [r["request_id"] for r in requests_data]

        # Count user_playbooks with those request_ids
        count_response = (
            self._table("user_playbooks")
            .select("request_id", count="exact")  # type: ignore[reportArgumentType]
            .in_("request_id", request_ids)
            .execute()
        )

        return count_response.count if count_response.count is not None else 0

    @handle_exceptions
    def delete_all_user_playbooks(self) -> None:
        self._table("user_playbooks").delete().gte("user_playbook_id", 0).execute()
        if self._opensearch:
            self._opensearch.delete_by_filter("user_playbooks", [])

    @handle_exceptions
    def delete_user_playbook(self, user_playbook_id: int) -> None:
        """Delete a user playbook by ID.

        Args:
            user_playbook_id (int): The ID of the user playbook to delete
        """
        self._table("user_playbooks").delete().eq(
            "user_playbook_id", user_playbook_id
        ).execute()
        if self._opensearch:
            self._opensearch.delete_ids("user_playbooks", [user_playbook_id])
        self._record_playbook_event(
            event_name="user_playbook_deleted",
            outcome="deleted",
            entity_type="user_playbook",
            entity_id=str(user_playbook_id),
        )

    @handle_exceptions
    def delete_all_user_playbooks_by_playbook_name(
        self, playbook_name: str, agent_version: str | None = None
    ) -> None:
        """
        Delete all user playbooks by playbook name from storage.

        Args:
            playbook_name (str): The playbook name to delete
            agent_version (str, optional): The agent version to filter by. If None, deletes all agent versions.
        """
        query = (
            self._table("user_playbooks").delete().eq("playbook_name", playbook_name)
        )

        # Add agent_version filter if specified
        if agent_version is not None:
            query = query.eq("agent_version", agent_version)

        response = query.execute()
        if self._opensearch:
            self._opensearch.delete_ids(
                "user_playbooks",
                [row.get("user_playbook_id") for row in _rows(response)],
            )

    @handle_exceptions
    def delete_user_playbooks_by_ids(
        self, user_playbook_ids: list[int], *, emit_hard_delete: bool = True
    ) -> int:
        """
        Delete user playbooks by their IDs.

        Args:
            user_playbook_ids: List of user_playbook_id values to delete

        Returns:
            int: Number of user playbooks deleted
        """
        _ = emit_hard_delete
        if not user_playbook_ids:
            return 0
        response = (
            self._table("user_playbooks")
            .delete()
            .in_("user_playbook_id", user_playbook_ids)
            .execute()
        )
        if self._opensearch:
            self._opensearch.delete_ids(
                "user_playbooks",
                [row.get("user_playbook_id") for row in _rows(response)],
            )
        for row in response.data:
            row_id = row.get("user_playbook_id")
            if row_id is None:
                continue
            self._record_playbook_event(
                event_name="user_playbook_deleted",
                outcome="deleted",
                entity_type="user_playbook",
                entity_id=str(row_id),
            )
        return len(response.data)

    @handle_exceptions
    def get_user_playbooks_by_ids(
        self,
        user_id: str,
        user_playbook_ids: list[int],
        status_filter: list[Status | None] | None = None,
    ) -> list[UserPlaybook]:
        """Fetch selected user playbooks for a user by id.

        See base class ``BaseStorage.get_user_playbooks_by_ids`` for the
        full contract; this is the Supabase-backed implementation.
        """
        if not user_playbook_ids:
            return []
        if status_filter is None:
            status_filter = [None]

        query = (
            self._table("user_playbooks")
            .select(_USER_PLAYBOOK_COLUMNS)
            .eq("user_id", user_id)
            .in_("user_playbook_id", user_playbook_ids)
        )
        query = _apply_status_filter_to_query(query, status_filter)

        response = query.execute()
        return [self._row_to_user_playbook(item) for item in _rows(response)]

    @handle_exceptions
    def get_user_playbook_by_id(
        self, user_playbook_id: int, *, include_tombstones: bool = False
    ) -> UserPlaybook | None:
        response = (
            self._table("user_playbooks")
            .select(_USER_PLAYBOOK_COLUMNS)
            .eq("user_playbook_id", user_playbook_id)
            .limit(1)
        )
        if not include_tombstones:
            response = _apply_status_filter_to_query(response, [None])
        result = response.execute()
        rows = _rows(result)
        return self._row_to_user_playbook(rows[0]) if rows else None

    @handle_exceptions
    def get_user_playbooks_by_ids_any_user(
        self,
        user_playbook_ids: list[int],
        status_filter: list[Status | None] | None = None,
    ) -> list[UserPlaybook]:
        if not user_playbook_ids:
            return []
        query = (
            self._table("user_playbooks")
            .select(_USER_PLAYBOOK_COLUMNS)
            .in_("user_playbook_id", user_playbook_ids)
        )
        if status_filter is not None:
            query = _apply_status_filter_to_query(query, status_filter)
        response = query.execute()
        by_id = {
            int(item["user_playbook_id"]): self._row_to_user_playbook(item)
            for item in _rows(response)
        }
        return [by_id[upid] for upid in user_playbook_ids if upid in by_id]

    @handle_exceptions
    def archive_user_playbook_by_id(self, user_id: str, user_playbook_id: int) -> bool:
        """Archive a single current user playbook, guarded by owner id.

        See base class ``BaseStorage.archive_user_playbook_by_id`` for the
        full contract; this is the Supabase-backed implementation.
        """
        response = (
            self._table("user_playbooks")
            .update({"status": Status.ARCHIVED.value})
            .eq("user_playbook_id", user_playbook_id)
            .eq("user_id", user_id)
            .is_("status", "null")
            .execute()
        )
        return len(_rows(response)) > 0

    @handle_exceptions
    def update_all_user_playbooks_status(
        self,
        old_status: Status | None,
        new_status: Status | None,
        agent_version: str | None = None,
        playbook_name: str | None = None,
    ) -> int:
        """
        Update all user playbooks with old_status to new_status atomically.

        Args:
            old_status: The current status to match (None for CURRENT)
            new_status: The new status to set (None for CURRENT)
            agent_version: Optional filter by agent version
            playbook_name: Optional filter by playbook name

        Returns:
            int: Number of user playbooks updated
        """
        # Build the update query
        query = self._table("user_playbooks").update(
            {"status": new_status.value if new_status else None}
        )

        # Apply old_status filter
        if old_status is None or (
            hasattr(old_status, "value") and old_status.value is None
        ):
            # Match CURRENT user playbooks (status IS NULL)
            query = query.is_("status", "null")
        else:
            # Match specific status
            query = query.eq("status", old_status.value)

        # Add optional filters
        if agent_version is not None:
            query = query.eq("agent_version", agent_version)
        if playbook_name is not None:
            query = query.eq("playbook_name", playbook_name)

        # Execute the update
        response = query.execute()

        # Count the number of rows updated
        updated_count = len(response.data) if response.data else 0
        logger.info(
            "Updated %s user playbooks from %s to %s",
            updated_count,
            old_status,
            new_status,
        )
        return updated_count

    @handle_exceptions
    def delete_all_user_playbooks_by_status(
        self,
        status: Status,
        agent_version: str | None = None,
        playbook_name: str | None = None,
    ) -> int:
        """
        Delete all user playbooks with the given status atomically.

        Args:
            status: The status of user playbooks to delete
            agent_version: Optional filter by agent version
            playbook_name: Optional filter by playbook name

        Returns:
            int: Number of user playbooks deleted
        """
        # Build the delete query
        query = self._table("user_playbooks").delete().eq("status", status.value)

        # Add optional filters
        if agent_version is not None:
            query = query.eq("agent_version", agent_version)
        if playbook_name is not None:
            query = query.eq("playbook_name", playbook_name)

        # Execute the delete
        response = query.execute()

        # Count the number of rows deleted
        deleted_count = len(response.data) if response.data else 0
        logger.info("Deleted %s user playbooks with status %s", deleted_count, status)
        return deleted_count

    @handle_exceptions
    def has_user_playbooks_with_status(
        self,
        status: Status | None,
        agent_version: str | None = None,
        playbook_name: str | None = None,
    ) -> bool:
        """
        Check if any user playbooks exist with given status and filters.

        Args:
            status: The status to check for (None for CURRENT)
            agent_version: Optional filter by agent version
            playbook_name: Optional filter by playbook name

        Returns:
            bool: True if any matching user playbooks exist
        """
        # Build the query to count matching user playbooks
        query = self._table("user_playbooks").select(
            "user_playbook_id",
            count="exact",  # type: ignore[reportArgumentType]
        )

        # Apply status filter
        if status is None or (hasattr(status, "value") and status.value is None):
            # Match CURRENT user playbooks (status IS NULL)
            query = query.is_("status", "null")
        else:
            # Match specific status
            query = query.eq("status", status.value)

        # Add optional filters
        if agent_version is not None:
            query = query.eq("agent_version", agent_version)
        if playbook_name is not None:
            query = query.eq("playbook_name", playbook_name)

        # Execute the query with limit 1 for efficiency
        response = query.limit(1).execute()

        return response.count is not None and response.count > 0

    @handle_exceptions
    def search_user_playbooks(  # noqa: C901
        self,
        request: SearchUserPlaybookRequest,
        options: SearchOptions | None = None,
    ) -> list[UserPlaybook]:
        """
        Search user playbooks with advanced filtering including semantic search.

        Args:
            request (SearchUserPlaybookRequest): Search request with query, filters, and search_mode
            options (SearchOptions, optional): Engine-level options (e.g., pre-computed embedding)

        Returns:
            list[UserPlaybook]: List of matching user playbook objects
        """
        query = request.query
        user_id = request.user_id
        agent_version = request.agent_version
        playbook_name = request.playbook_name
        start_time = int(request.start_time.timestamp()) if request.start_time else None
        end_time = int(request.end_time.timestamp()) if request.end_time else None
        status_filter = request.status_filter
        match_threshold = request.threshold or 0.5
        match_count = request.top_k or 10
        query_embedding = options.query_embedding if options else None

        # If query is provided, use hybrid search first (filters applied in Python)
        if query:
            effective_mode = request.search_mode or self.search_mode
            if self._opensearch:
                filters: list[dict[str, Any]] = []
                if user_id:
                    filters.append({"term": {"user_id": user_id}})
                if agent_version:
                    filters.append({"term": {"agent_version": agent_version}})
                if playbook_name:
                    filters.append({"term": {"playbook_name": playbook_name}})
                if start_time:
                    filters.append({"range": {"created_at": {"gte": start_time}}})
                if end_time:
                    filters.append({"range": {"created_at": {"lte": end_time}}})
                terms = status_filter_terms(status_filter)
                if terms is not None:
                    filters.append({"terms": {"status": terms}})
                else:
                    filters.append(
                        {
                            "bool": {
                                "must_not": [
                                    {"terms": {"status": ["merged", "superseded"]}}
                                ]
                            }
                        }
                    )
                if request.tags:
                    filters.append({"terms": {"tags": request.tags}})
                ids = self._opensearch.search_ids(
                    entity="user_playbooks",
                    query_text=query,
                    query_embedding=query_embedding or self._get_embedding(query),
                    search_mode=effective_mode,
                    top_k=match_count,
                    threshold=match_threshold,
                    filters=filters,
                )
                playbooks = self.get_user_playbooks_by_ids_any_user(
                    [int(playbook_id) for playbook_id in ids]
                )
                return _order_by_ids(playbooks, ids, "user_playbook_id")
            response = self._rpc(
                "hybrid_match_user_playbooks",
                {
                    "p_query_embedding": query_embedding or self._get_embedding(query),
                    "p_query_text": query,
                    "p_match_threshold": match_threshold,
                    "p_match_count": match_count
                    * 10,  # Get more results to allow for filtering
                    "p_filter_user_id": user_id,
                    "p_search_mode": effective_mode.value,
                    "p_rrf_k": 60,
                    "p_vector_weight": self.vector_weight,
                    "p_fts_weight": self.fts_weight,
                },
            ).execute()
            data = cast(list[dict[str, Any]], response.data)
            user_playbooks = [self._row_to_user_playbook(item) for item in data]

            # Apply filters in Python for RPC results
            filtered_playbooks = []
            for up in user_playbooks:
                if agent_version and up.agent_version != agent_version:
                    continue
                if playbook_name and up.playbook_name != playbook_name:
                    continue
                if start_time and up.created_at < start_time:
                    continue
                if end_time and up.created_at > end_time:
                    continue
                if status_filter is not None and not matches_status_filter(
                    up.status, status_filter
                ):
                    continue
                filtered_playbooks.append(up)
            return filtered_playbooks[:match_count]

        # No query - use regular table query with Supabase filters
        # For the non-RPC path, resolve user_id to request_ids via the requests table
        request_ids_for_user: list[str] | None = None
        if user_id:
            requests_response = (
                self._table("requests")
                .select("request_id")
                .eq("user_id", user_id)
                .execute()
            )
            request_ids_for_user = [r["request_id"] for r in _rows(requests_response)]
            if not request_ids_for_user:
                return []

        db_query = (
            self._table("user_playbooks")
            .select(_USER_PLAYBOOK_COLUMNS)
            .order("created_at", desc=True)
            .limit(match_count)
        )

        # Apply filters at database level
        if request_ids_for_user is not None:
            db_query = db_query.in_("request_id", request_ids_for_user)
        if agent_version:
            db_query = db_query.eq("agent_version", agent_version)
        if playbook_name:
            db_query = db_query.eq("playbook_name", playbook_name)
        if start_time:
            db_query = db_query.gte("created_at", _timestamp_to_iso(start_time))
        if end_time:
            db_query = db_query.lte("created_at", _timestamp_to_iso(end_time))
        if status_filter is not None:
            or_condition = _build_status_or_condition(status_filter)
            if or_condition:
                db_query = db_query.or_(or_condition)

        response = db_query.execute()
        return [self._row_to_user_playbook(item) for item in _rows(response)]

    # ==============================
    # Agent Playbook methods
    # ==============================

    @handle_exceptions
    def save_agent_playbooks(
        self, agent_playbooks: list[AgentPlaybook]
    ) -> list[AgentPlaybook]:
        """
        Save agent playbooks with embeddings.

        Args:
            agent_playbooks (list[AgentPlaybook]): List of agent playbook objects to save

        Returns:
            list[AgentPlaybook]: Saved agent playbooks with agent_playbook_id populated from storage
        """
        saved_playbooks = []
        for agent_playbook in agent_playbooks:
            is_new = not agent_playbook.agent_playbook_id
            embedding_text = agent_playbook.trigger or agent_playbook.content
            if self._should_expand_documents():
                with ThreadPoolExecutor(max_workers=2) as executor:
                    emb_future = executor.submit(self._get_embedding, embedding_text)
                    exp_future = executor.submit(self._expand_document, embedding_text)
                    agent_playbook.embedding = emb_future.result(timeout=15)
                    agent_playbook.expanded_terms = exp_future.result(timeout=15)
            else:
                agent_playbook.embedding = self._get_embedding(embedding_text)
            response = (
                self._table("agent_playbooks")
                .upsert(agent_playbook_to_data(agent_playbook))
                .execute()
            )
            if response.data and isinstance(response.data, list):
                row = cast(dict[str, Any], response.data[0])
                agent_playbook.agent_playbook_id = row.get(
                    "agent_playbook_id", agent_playbook.agent_playbook_id
                )
            if self._opensearch:
                self._opensearch.index_rows("agent_playbooks", _rows(response))
            saved_playbooks.append(agent_playbook)
            self._record_playbook_event(
                event_name=(
                    "agent_playbook_created" if is_new else "agent_playbook_updated"
                ),
                outcome="created" if is_new else "updated",
                entity_type="agent_playbook",
                entity_id=(
                    str(agent_playbook.agent_playbook_id)
                    if agent_playbook.agent_playbook_id
                    else None
                ),
                playbook_name=agent_playbook.playbook_name,
                agent_version=agent_playbook.agent_version,
            )
        return saved_playbooks

    @handle_exceptions
    def get_agent_playbooks(
        self,
        limit: int = 100,
        playbook_name: str | None = None,
        agent_version: str | None = None,
        status_filter: list[Status | None] | None = None,
        playbook_status_filter: list[PlaybookStatus] | None = None,
        tags: list[str] | None = None,
    ) -> list[AgentPlaybook]:
        """
        Get agent playbooks from storage.

        Args:
            limit (int): Maximum number of agent playbooks to return
            playbook_name (str, optional): The playbook name to filter by. If None, returns all agent playbooks.
            agent_version (str, optional): The agent version to filter by. If None, returns all versions.
            status_filter (list[Optional[Status]], optional): List of Status values to filter by. None in the list means CURRENT status.
            playbook_status_filter (Optional[list[PlaybookStatus]]): List of PlaybookStatus values to filter by.
                If None, returns all playbook statuses.

        Returns:
            list[AgentPlaybook]: List of agent playbook objects
        """
        query = (
            self._table("agent_playbooks")
            .select(_AGENT_PLAYBOOK_COLUMNS)
            .order("created_at", desc=True)
            .limit(limit)
        )

        # Add playbook_name filter if specified (skip if None or empty string)
        if playbook_name:
            query = query.eq("playbook_name", playbook_name)

        if agent_version is not None:
            query = query.eq("agent_version", agent_version)

        # Apply status filter (for Status: CURRENT, ARCHIVED, PENDING, etc.)
        if status_filter is not None:
            has_none = False
            status_strings = []
            for s in status_filter:
                if s is None or (hasattr(s, "value") and s.value is None):
                    has_none = True
                elif isinstance(s, Status):
                    status_strings.append(s.value)
                elif isinstance(s, str):
                    status_strings.append(s)
            if has_none and status_strings:
                query = query.or_(
                    f"status.is.null,status.in.({','.join(status_strings)})"
                )
            elif has_none:
                query = query.is_("status", "null")
            elif status_strings:
                query = query.in_("status", status_strings)
        else:
            # Default behavior: exclude archived (keep current agent playbooks)
            query = query.is_("status", "null")

        # Apply playbook_status filter (for PlaybookStatus: PENDING, APPROVED, REJECTED)
        # Only apply if specified; when None or empty, return all playbook statuses
        if playbook_status_filter:
            status_values = [
                s.value if isinstance(s, PlaybookStatus) else s
                for s in playbook_status_filter
            ]
            query = query.in_("playbook_status", status_values)
        if tags:
            query = query.contains("tags", tags)

        response = query.execute()
        return [self._row_to_agent_playbook(item) for item in _rows(response)]

    @handle_exceptions
    def get_agent_playbook_by_id(
        self, agent_playbook_id: int, *, include_tombstones: bool = False
    ) -> AgentPlaybook | None:
        query = (
            self._table("agent_playbooks")
            .select(_AGENT_PLAYBOOK_COLUMNS)
            .eq("agent_playbook_id", agent_playbook_id)
            .limit(1)
        )
        if not include_tombstones:
            query = _apply_status_filter_to_query(query, [None])
        response = query.execute()
        rows = _rows(response)
        return self._row_to_agent_playbook(rows[0]) if rows else None

    @handle_exceptions
    def delete_all_agent_playbooks(self) -> None:
        self._table("agent_playbooks").delete().gte("agent_playbook_id", 0).execute()
        if self._opensearch:
            self._opensearch.delete_by_filter("agent_playbooks", [])

    @handle_exceptions
    def delete_agent_playbook(self, agent_playbook_id: int) -> None:
        """Delete an agent playbook by ID.

        Args:
            agent_playbook_id (int): The ID of the agent playbook to delete
        """
        self._table("agent_playbooks").delete().eq(
            "agent_playbook_id", agent_playbook_id
        ).execute()
        if self._opensearch:
            self._opensearch.delete_ids("agent_playbooks", [agent_playbook_id])
        self._record_playbook_event(
            event_name="agent_playbook_deleted",
            outcome="deleted",
            entity_type="agent_playbook",
            entity_id=str(agent_playbook_id),
        )

    @handle_exceptions
    def delete_all_agent_playbooks_by_playbook_name(
        self, playbook_name: str, agent_version: str | None = None
    ) -> None:
        """
        Delete all agent playbooks by playbook name from storage.

        Args:
            playbook_name (str): The playbook name to delete
            agent_version (str, optional): The agent version to filter by. If None, deletes all agent versions.
        """
        query = (
            self._table("agent_playbooks").delete().eq("playbook_name", playbook_name)
        )

        # Add agent_version filter if specified
        if agent_version is not None:
            query = query.eq("agent_version", agent_version)

        response = query.execute()
        if self._opensearch:
            self._opensearch.delete_ids(
                "agent_playbooks",
                [row.get("agent_playbook_id") for row in _rows(response)],
            )

    @handle_exceptions
    def delete_agent_playbooks_by_ids(
        self, agent_playbook_ids: list[int], *, emit_hard_delete: bool = True
    ) -> None:
        """
        Permanently delete agent playbooks by their IDs.
        No-op if agent_playbook_ids is empty.

        Args:
            agent_playbook_ids (list[int]): List of agent playbook IDs to delete
        """
        _ = emit_hard_delete
        if not agent_playbook_ids:
            return
        response = (
            self._table("agent_playbooks")
            .delete()
            .in_("agent_playbook_id", agent_playbook_ids)
            .execute()
        )
        if self._opensearch:
            self._opensearch.delete_ids(
                "agent_playbooks",
                [row.get("agent_playbook_id") for row in _rows(response)],
            )
        for row in response.data:
            row_id = row.get("agent_playbook_id")
            if row_id is None:
                continue
            self._record_playbook_event(
                event_name="agent_playbook_deleted",
                outcome="deleted",
                entity_type="agent_playbook",
                entity_id=str(row_id),
            )

    @handle_exceptions
    def update_agent_playbook_status(
        self, agent_playbook_id: int, playbook_status: PlaybookStatus
    ) -> None:
        """
        Update the status of a specific agent playbook.

        Args:
            agent_playbook_id (int): The ID of the agent playbook to update
            playbook_status (PlaybookStatus): The new status to set

        Raises:
            ValueError: If agent playbook with the given ID is not found
        """
        # Check if agent playbook exists
        response = (
            self._table("agent_playbooks")
            .select("agent_playbook_id")
            .eq("agent_playbook_id", agent_playbook_id)
            .execute()
        )

        if not response.data:
            raise ValueError(f"Agent playbook with ID {agent_playbook_id} not found")

        # Update the playbook status
        response = (
            self._table("agent_playbooks")
            .update({"playbook_status": playbook_status.value})
            .eq("agent_playbook_id", agent_playbook_id)
            .execute()
        )
        if self._opensearch:
            self._opensearch.index_rows("agent_playbooks", _rows(response))

    @handle_exceptions
    def update_agent_playbook(
        self,
        agent_playbook_id: int,
        playbook_name: str | None = None,
        content: str | None = None,
        trigger: str | None = None,
        rationale: str | None = None,
        blocking_issue: BlockingIssue | None = None,
        playbook_status: PlaybookStatus | None = None,
    ) -> None:
        """Update editable fields of an agent playbook. Only non-None fields are updated.

        Args:
            agent_playbook_id (int): The ID of the agent playbook to update
            playbook_name (str, optional): New playbook name
            content (str, optional): New content text
            trigger (str, optional): New trigger text
            rationale (str, optional): New rationale text
            blocking_issue (BlockingIssue, optional): New blocking issue
            playbook_status (PlaybookStatus, optional): New playbook status

        Raises:
            ValueError: If agent playbook with the given ID is not found
        """
        response = (
            self._table("agent_playbooks")
            .select("agent_playbook_id")
            .eq("agent_playbook_id", agent_playbook_id)
            .execute()
        )

        if not response.data:
            raise ValueError(f"Agent playbook with ID {agent_playbook_id} not found")

        updates: dict[str, Any] = {}
        if playbook_name is not None:
            updates["playbook_name"] = playbook_name
        if content is not None:
            updates["content"] = content
        if trigger is not None:
            updates["trigger"] = trigger
        if rationale is not None:
            updates["rationale"] = rationale
        if blocking_issue is not None:
            updates["blocking_issue"] = blocking_issue.model_dump()
        if playbook_status is not None:
            updates["playbook_status"] = playbook_status.value
        if updates:
            response = (
                self._table("agent_playbooks")
                .update(updates)
                .eq("agent_playbook_id", agent_playbook_id)
                .execute()
            )
            if self._opensearch:
                self._opensearch.index_rows("agent_playbooks", _rows(response))
            self._record_playbook_event(
                event_name="agent_playbook_updated",
                outcome="updated",
                entity_type="agent_playbook",
                entity_id=str(agent_playbook_id),
                playbook_name=playbook_name,
                metadata={"updated_fields": sorted(updates)},
            )

    @handle_exceptions
    def update_user_playbook(
        self,
        user_playbook_id: int,
        playbook_name: str | None = None,
        content: str | None = None,
        trigger: str | None = None,
        rationale: str | None = None,
        blocking_issue: BlockingIssue | None = None,
    ) -> None:
        """Update editable fields of a user playbook. Only non-None fields are updated.

        Args:
            user_playbook_id (int): The ID of the user playbook to update
            playbook_name (str, optional): New playbook name
            content (str, optional): New content text
            trigger (str, optional): New trigger text
            rationale (str, optional): New rationale text
            blocking_issue (BlockingIssue, optional): New blocking issue

        Raises:
            ValueError: If user playbook with the given ID is not found
        """
        response = (
            self._table("user_playbooks")
            .select("user_playbook_id")
            .eq("user_playbook_id", user_playbook_id)
            .execute()
        )

        if not response.data:
            raise ValueError(f"User playbook with ID {user_playbook_id} not found")

        updates: dict[str, Any] = {}
        if playbook_name is not None:
            updates["playbook_name"] = playbook_name
        if content is not None:
            updates["content"] = content
        if trigger is not None:
            updates["trigger"] = trigger
        if rationale is not None:
            updates["rationale"] = rationale
        if blocking_issue is not None:
            updates["blocking_issue"] = blocking_issue.model_dump()
        if updates:
            response = (
                self._table("user_playbooks")
                .update(updates)
                .eq("user_playbook_id", user_playbook_id)
                .execute()
            )
            if self._opensearch:
                self._opensearch.index_rows("user_playbooks", _rows(response))
            self._record_playbook_event(
                event_name="user_playbook_updated",
                outcome="updated",
                entity_type="user_playbook",
                entity_id=str(user_playbook_id),
                playbook_name=playbook_name,
                metadata={"updated_fields": sorted(updates)},
            )

    @handle_exceptions
    def archive_agent_playbooks_by_playbook_name(
        self, playbook_name: str, agent_version: str | None = None
    ) -> None:
        """
        Archive non-APPROVED agent playbooks by setting their status field to 'archived'.
        APPROVED agent playbooks are left untouched to preserve user-approved playbooks.

        Args:
            playbook_name (str): The playbook name to archive
            agent_version (str, optional): The agent version to filter by. If None, archives all agent versions.
        """
        query = (
            self._table("agent_playbooks")
            .update({"status": "archived"})
            .eq("playbook_name", playbook_name)
            .neq("playbook_status", PlaybookStatus.APPROVED.value)
        )

        # Add agent_version filter if specified
        if agent_version is not None:
            query = query.eq("agent_version", agent_version)

        response = query.execute()
        if self._opensearch:
            self._opensearch.index_rows("agent_playbooks", _rows(response))

    @handle_exceptions
    def archive_agent_playbooks_by_ids(self, agent_playbook_ids: list[int]) -> None:
        """
        Archive non-APPROVED agent playbooks by IDs, setting their status field to 'archived'.
        APPROVED agent playbooks are left untouched. No-op if agent_playbook_ids is empty.

        Args:
            agent_playbook_ids (list[int]): List of agent playbook IDs to archive
        """
        if not agent_playbook_ids:
            return
        response = (
            self._table("agent_playbooks")
            .update({"status": "archived"})
            .in_("agent_playbook_id", agent_playbook_ids)
            .neq("playbook_status", PlaybookStatus.APPROVED.value)
            .execute()
        )
        if self._opensearch:
            self._opensearch.index_rows("agent_playbooks", _rows(response))

    @handle_exceptions
    def restore_archived_agent_playbooks_by_playbook_name(
        self, playbook_name: str, agent_version: str | None = None
    ) -> None:
        """
        Restore archived agent playbooks by setting their status field to null.

        Args:
            playbook_name (str): The playbook name to restore
            agent_version (str, optional): The agent version to filter by. If None, restores all agent versions.
        """
        query = (
            self._table("agent_playbooks")
            .update({"status": None})
            .eq("playbook_name", playbook_name)
            .eq("status", "archived")
        )

        # Add agent_version filter if specified
        if agent_version is not None:
            query = query.eq("agent_version", agent_version)

        response = query.execute()
        if self._opensearch:
            self._opensearch.index_rows("agent_playbooks", _rows(response))

    @handle_exceptions
    def restore_archived_agent_playbooks_by_ids(
        self, agent_playbook_ids: list[int]
    ) -> None:
        """
        Restore archived agent playbooks by IDs, setting their status field to null.
        No-op if agent_playbook_ids is empty.

        Args:
            agent_playbook_ids (list[int]): List of agent playbook IDs to restore
        """
        if not agent_playbook_ids:
            return
        response = (
            self._table("agent_playbooks")
            .update({"status": None})
            .in_("agent_playbook_id", agent_playbook_ids)
            .eq("status", "archived")
            .execute()
        )
        if self._opensearch:
            self._opensearch.index_rows("agent_playbooks", _rows(response))

    def _supersede_rows_by_ids(
        self,
        *,
        table: str,
        pk: str,
        entity_type: str,
        ids: list[int],
        request_id: str,
        exclude_approved: bool = False,
    ) -> int:
        if not ids:
            return 0
        now = int(datetime.now(UTC).timestamp())
        approved_guard = (
            sql.SQL(" AND playbook_status <> %s") if exclude_approved else sql.SQL("")
        )
        params: list[Any] = [
            Status.SUPERSEDED.value,
            now,
            ids,
            Status.MERGED.value,
            Status.SUPERSEDED.value,
        ]
        if exclude_approved:
            params.append(PlaybookStatus.APPROVED.value)
        rows = self._fetch_all(
            sql.SQL(
                "UPDATE {} SET status = %s, retired_at = %s "
                "WHERE {} = ANY(%s) "
                "AND (status IS NULL OR status NOT IN (%s, %s))"
            )
            .format(self._table_identifier(table), sql.Identifier(pk))
            + approved_guard
            + sql.SQL(" RETURNING {}").format(sql.Identifier(pk)),
            params,
        )
        changed_ids = [int(row[pk]) for row in rows]
        for changed_id in changed_ids:
            self._fetch_all(
                sql.SQL(
                    """
                    INSERT INTO {} (
                        org_id, entity_type, entity_id, op, prov_relation,
                        source_ids, actor, request_id, reason, created_at,
                        from_status, to_status, status_namespace
                    )
                    VALUES (%s, %s, %s, 'status_change', 'wasInvalidatedBy',
                            '[]'::jsonb, 'system', %s, 'soft_supersede',
                            %s, NULL, %s, 'lifecycle')
                    ON CONFLICT (org_id, entity_type, entity_id, op, request_id)
                    DO NOTHING
                    RETURNING 1
                    """
                ).format(self._table_identifier("lineage_event")),
                [
                    self.org_id,
                    entity_type,
                    str(changed_id),
                    request_id,
                    now,
                    Status.SUPERSEDED.value,
                ],
            )
        if changed_ids and self._opensearch:
            response = (
                self._table(table)
                .select("*")
                .in_(pk, changed_ids)
                .execute()
            )
            self._opensearch.index_rows(table, _rows(response))
        return len(changed_ids)

    @handle_exceptions
    def supersede_user_playbooks_by_ids(
        self, user_playbook_ids: list[int], request_id: str
    ) -> int:
        return self._supersede_rows_by_ids(
            table="user_playbooks",
            pk="user_playbook_id",
            entity_type="user_playbook",
            ids=user_playbook_ids,
            request_id=request_id,
        )

    @handle_exceptions
    def supersede_agent_playbooks_by_ids(
        self, agent_playbook_ids: list[int], request_id: str
    ) -> int:
        return self._supersede_rows_by_ids(
            table="agent_playbooks",
            pk="agent_playbook_id",
            entity_type="agent_playbook",
            ids=agent_playbook_ids,
            request_id=request_id,
            exclude_approved=True,
        )

    @handle_exceptions
    def supersede_agent_playbooks_by_playbook_name(
        self, playbook_name: str, agent_version: str | None, request_id: str
    ) -> int:
        query = (
            self._table("agent_playbooks")
            .select("agent_playbook_id")
            .eq("playbook_name", playbook_name)
            .eq("status", Status.ARCHIVED.value)
        )
        if agent_version is not None:
            query = query.eq("agent_version", agent_version)
        ids = [int(row["agent_playbook_id"]) for row in _rows(query.execute())]
        return self.supersede_agent_playbooks_by_ids(ids, request_id)

    @handle_exceptions
    def delete_archived_agent_playbooks_by_playbook_name(
        self, playbook_name: str, agent_version: str | None = None
    ) -> None:
        """
        Permanently delete agent playbooks that have status='archived'.

        Args:
            playbook_name (str): The playbook name to delete
            agent_version (str, optional): The agent version to filter by. If None, deletes all agent versions.
        """
        query = (
            self._table("agent_playbooks")
            .delete()
            .eq("playbook_name", playbook_name)
            .eq("status", "archived")
        )

        # Add agent_version filter if specified
        if agent_version is not None:
            query = query.eq("agent_version", agent_version)

        response = query.execute()
        if self._opensearch:
            self._opensearch.delete_ids(
                "agent_playbooks",
                [row.get("agent_playbook_id") for row in _rows(response)],
            )

    @handle_exceptions
    def search_agent_playbooks(  # noqa: C901
        self,
        request: SearchAgentPlaybookRequest,
        options: SearchOptions | None = None,
    ) -> list[AgentPlaybook]:
        """
        Search agent playbooks with advanced filtering including semantic search.

        Args:
            request (SearchAgentPlaybookRequest): Search request with query, filters, and search_mode
            options (SearchOptions, optional): Engine-level options (e.g., pre-computed embedding)

        Returns:
            list[AgentPlaybook]: List of matching agent playbook objects
        """
        query = request.query
        agent_version = request.agent_version
        playbook_name = request.playbook_name
        start_time = int(request.start_time.timestamp()) if request.start_time else None
        end_time = int(request.end_time.timestamp()) if request.end_time else None
        status_filter = request.status_filter
        playbook_status_filter = request.playbook_status_filter
        match_threshold = request.threshold or 0.5
        match_count = request.top_k or 10
        query_embedding = options.query_embedding if options else None

        # If query is provided, use hybrid search first (filters applied in Python)
        if query:
            effective_mode = request.search_mode or self.search_mode
            if self._opensearch:
                filters = []
                if agent_version:
                    filters.append({"term": {"agent_version": agent_version}})
                if playbook_name:
                    filters.append({"term": {"playbook_name": playbook_name}})
                if start_time:
                    filters.append({"range": {"created_at": {"gte": start_time}}})
                if end_time:
                    filters.append({"range": {"created_at": {"lte": end_time}}})
                terms = status_filter_terms(status_filter)
                if terms is not None:
                    filters.append({"terms": {"status": terms}})
                else:
                    filters.append(
                        {
                            "bool": {
                                "must_not": [
                                    {"terms": {"status": ["merged", "superseded"]}}
                                ]
                            }
                        }
                    )
                if playbook_status_filter is not None:
                    if isinstance(playbook_status_filter, list):
                        playbook_status_terms = [
                            status.value for status in playbook_status_filter
                        ]
                    else:
                        playbook_status_terms = [playbook_status_filter.value]
                    filters.append(
                        {"terms": {"playbook_status": playbook_status_terms}}
                    )
                if request.tags:
                    filters.append({"terms": {"tags": request.tags}})
                ids = self._opensearch.search_ids(
                    entity="agent_playbooks",
                    query_text=query,
                    query_embedding=query_embedding or self._get_embedding(query),
                    search_mode=effective_mode,
                    top_k=match_count,
                    threshold=match_threshold,
                    filters=filters,
                )
                if not ids:
                    return []
                response = (
                    self._table("agent_playbooks")
                    .select(_AGENT_PLAYBOOK_COLUMNS)
                    .in_("agent_playbook_id", [int(playbook_id) for playbook_id in ids])
                    .execute()
                )
                playbooks = [
                    self._row_to_agent_playbook(item) for item in _rows(response)
                ]
                return _order_by_ids(playbooks, ids, "agent_playbook_id")
            response = self._rpc(
                "hybrid_match_agent_playbooks",
                {
                    "p_query_embedding": query_embedding or self._get_embedding(query),
                    "p_query_text": query,
                    "p_match_threshold": match_threshold,
                    "p_match_count": match_count
                    * 10,  # Get more results to allow for filtering
                    "p_search_mode": effective_mode.value,
                    "p_rrf_k": 60,
                    "p_vector_weight": self.vector_weight,
                    "p_fts_weight": self.fts_weight,
                },
            ).execute()
            data = cast(list[dict[str, Any]], response.data)
            agent_playbooks = [self._row_to_agent_playbook(item) for item in data]

            # Apply filters in Python for RPC results.
            # playbook_status_filter is `PlaybookStatus | list[PlaybookStatus] | None`;
            # normalize to a set of `PlaybookStatus` enum values for uniform
            # membership checks below.
            if playbook_status_filter is None:
                allowed_playbook_statuses: set[PlaybookStatus] | None = None
            elif isinstance(playbook_status_filter, list):
                allowed_playbook_statuses = set(playbook_status_filter)
            else:
                allowed_playbook_statuses = {playbook_status_filter}
            filtered_playbooks = []
            for ap in agent_playbooks:
                if agent_version and ap.agent_version != agent_version:
                    continue
                if playbook_name and ap.playbook_name != playbook_name:
                    continue
                if start_time and ap.created_at < start_time:
                    continue
                if end_time and ap.created_at > end_time:
                    continue
                if (
                    allowed_playbook_statuses is not None
                    and ap.playbook_status not in allowed_playbook_statuses
                ):
                    continue
                if status_filter is not None and not matches_status_filter(
                    ap.status, status_filter
                ):
                    continue
                filtered_playbooks.append(ap)
            return filtered_playbooks[:match_count]

        # No query - use regular table query with Supabase filters
        db_query = (
            self._table("agent_playbooks")
            .select(_AGENT_PLAYBOOK_COLUMNS)
            .order("created_at", desc=True)
            .limit(match_count)
        )

        # Apply filters at database level
        if agent_version:
            db_query = db_query.eq("agent_version", agent_version)
        if playbook_name:
            db_query = db_query.eq("playbook_name", playbook_name)
        if start_time:
            db_query = db_query.gte("created_at", _timestamp_to_iso(start_time))
        if end_time:
            db_query = db_query.lte("created_at", _timestamp_to_iso(end_time))
        if playbook_status_filter:
            # playbook_status_filter is `PlaybookStatus | list[PlaybookStatus]`
            # here (None is excluded by the truthiness check). Use `IN` for
            # lists (matches sqlite/postgres behavior) and `=` for a scalar.
            if isinstance(playbook_status_filter, list):
                db_query = db_query.in_(
                    "playbook_status", [s.value for s in playbook_status_filter]
                )
            else:
                db_query = db_query.eq("playbook_status", playbook_status_filter.value)
        if status_filter is not None:
            or_condition = _build_status_or_condition(status_filter)
            if or_condition:
                db_query = db_query.or_(or_condition)

        response = db_query.execute()
        return [self._row_to_agent_playbook(item) for item in _rows(response)]

    # ==============================
    # Playbook optimization methods
    # ==============================

    @handle_exceptions
    def set_source_user_playbook_ids_for_agent_playbook(
        self, agent_playbook_id: int, user_playbook_ids: list[int]
    ) -> None:
        self.set_source_windows_for_agent_playbook(
            agent_playbook_id,
            [
                AgentPlaybookSourceWindow(
                    user_playbook_id=upid, source_interaction_ids=[]
                )
                for upid in user_playbook_ids
            ],
        )

    @handle_exceptions
    def get_source_user_playbook_ids_for_agent_playbook(
        self, agent_playbook_id: int
    ) -> list[int]:
        return [
            window.user_playbook_id
            for window in self.get_source_windows_for_agent_playbook(agent_playbook_id)
        ]

    @handle_exceptions
    def get_source_user_playbook_ids_for_agent_playbooks(
        self, agent_playbook_ids: Sequence[int]
    ) -> dict[int, list[int]]:
        if not agent_playbook_ids:
            return {}
        unique_ids = list(dict.fromkeys(int(agent_id) for agent_id in agent_playbook_ids))
        response = (
            self._table("agent_playbook_source_user_playbooks")
            .select("agent_playbook_id, user_playbook_id")
            .in_("agent_playbook_id", unique_ids)
            .order("agent_playbook_id", desc=False)
            .execute()
        )
        by_agent_id: dict[int, list[int]] = {agent_id: [] for agent_id in unique_ids}
        seen_by_agent_id: dict[int, set[int]] = {
            agent_id: set() for agent_id in unique_ids
        }
        for row in _rows(response):
            agent_playbook_id = int(row["agent_playbook_id"])
            user_playbook_id = int(row["user_playbook_id"])
            seen = seen_by_agent_id.setdefault(agent_playbook_id, set())
            if user_playbook_id not in seen:
                by_agent_id.setdefault(agent_playbook_id, []).append(user_playbook_id)
                seen.add(user_playbook_id)
        return by_agent_id

    @handle_exceptions
    def set_source_windows_for_agent_playbook(
        self,
        agent_playbook_id: int,
        source_windows: list[AgentPlaybookSourceWindow],
    ) -> None:
        by_id: dict[int, list[int]] = {}
        for window in source_windows:
            ids = by_id.setdefault(window.user_playbook_id, [])
            seen = set(ids)
            for source_id in window.source_interaction_ids:
                if source_id not in seen:
                    ids.append(source_id)
                    seen.add(source_id)
        self._table("agent_playbook_source_user_playbooks").delete().eq(
            "agent_playbook_id", agent_playbook_id
        ).execute()
        if not by_id:
            return
        self._table("agent_playbook_source_user_playbooks").insert(
            [
                {
                    "agent_playbook_id": agent_playbook_id,
                    "user_playbook_id": upid,
                    "source_interaction_ids": source_interaction_ids,
                }
                for upid, source_interaction_ids in by_id.items()
            ]
        ).execute()

    @handle_exceptions
    def get_source_windows_for_agent_playbook(
        self, agent_playbook_id: int
    ) -> list[AgentPlaybookSourceWindow]:
        response = (
            self._table("agent_playbook_source_user_playbooks")
            .select("user_playbook_id, source_interaction_ids")
            .eq("agent_playbook_id", agent_playbook_id)
            .order("user_playbook_id", desc=False)
            .execute()
        )
        return [
            AgentPlaybookSourceWindow(
                user_playbook_id=int(row["user_playbook_id"]),
                source_interaction_ids=_int_list(row.get("source_interaction_ids")),
            )
            for row in _rows(response)
        ]

    @handle_exceptions
    def create_playbook_optimization_job(
        self, job: PlaybookOptimizationJob
    ) -> PlaybookOptimizationJob:
        response = (
            self._table("playbook_optimization_jobs")
            .insert(
                {
                    "target_kind": job.target_kind,
                    "target_id": job.target_id,
                    "status": job.status,
                    "best_candidate_id": job.best_candidate_id,
                    "successor_target_id": job.successor_target_id,
                    "decision_reason": job.decision_reason,
                    "metadata_json": _json_data(job.metadata_json),
                    "created_at": job.created_at,
                    "updated_at": job.updated_at,
                }
            )
            .execute()
        )
        rows = _rows(response)
        if rows:
            job.job_id = int(rows[0]["job_id"])
        return job

    @handle_exceptions
    def update_playbook_optimization_job(
        self,
        job_id: int,
        *,
        status: str | None = None,
        best_candidate_id: int | None = None,
        successor_target_id: int | None = None,
        decision_reason: str | None = None,
        metadata_json: str | None = None,
    ) -> None:
        updates: dict[str, Any] = {"updated_at": int(datetime.now(UTC).timestamp())}
        if status is not None:
            updates["status"] = status
        if best_candidate_id is not None:
            updates["best_candidate_id"] = best_candidate_id
        if successor_target_id is not None:
            updates["successor_target_id"] = successor_target_id
        if decision_reason is not None:
            updates["decision_reason"] = decision_reason
        if metadata_json is not None:
            updates["metadata_json"] = _json_data(metadata_json)
        self._table("playbook_optimization_jobs").update(updates).eq(
            "job_id", job_id
        ).execute()

    @handle_exceptions
    def insert_playbook_optimization_candidate(
        self, candidate: PlaybookOptimizationCandidate
    ) -> PlaybookOptimizationCandidate:
        response = (
            self._table("playbook_optimization_candidates")
            .insert(
                {
                    "job_id": candidate.job_id,
                    "candidate_index": candidate.candidate_index,
                    "content": candidate.content,
                    "parent_candidate_ids": candidate.parent_candidate_ids,
                    "aggregate_score": candidate.aggregate_score,
                    "is_winner": candidate.is_winner,
                    "created_at": candidate.created_at,
                }
            )
            .execute()
        )
        rows = _rows(response)
        if rows:
            candidate.candidate_id = int(rows[0]["candidate_id"])
        return candidate

    @handle_exceptions
    def list_playbook_optimization_candidates(
        self, job_id: int
    ) -> list[PlaybookOptimizationCandidate]:
        response = (
            self._table("playbook_optimization_candidates")
            .select("*")
            .eq("job_id", job_id)
            .order("candidate_id", desc=False)
            .execute()
        )
        return [
            PlaybookOptimizationCandidate(
                candidate_id=int(row["candidate_id"]),
                job_id=int(row["job_id"]),
                candidate_index=int(row["candidate_index"]),
                content=row["content"],
                parent_candidate_ids=row.get("parent_candidate_ids") or [],
                aggregate_score=row.get("aggregate_score"),
                is_winner=bool(row.get("is_winner")),
                created_at=int(row["created_at"]),
            )
            for row in _rows(response)
        ]

    @handle_exceptions
    def update_playbook_optimization_candidate(
        self,
        candidate_id: int,
        *,
        aggregate_score: float | None = None,
        is_winner: bool | None = None,
    ) -> None:
        updates: dict[str, Any] = {}
        if aggregate_score is not None:
            updates["aggregate_score"] = aggregate_score
        if is_winner is not None:
            updates["is_winner"] = is_winner
        if not updates:
            return
        self._table("playbook_optimization_candidates").update(updates).eq(
            "candidate_id", candidate_id
        ).execute()

    @handle_exceptions
    def insert_playbook_optimization_evaluation(
        self, evaluation: PlaybookOptimizationEvaluation
    ) -> PlaybookOptimizationEvaluation:
        response = (
            self._table("playbook_optimization_evaluations")
            .insert(
                {
                    "job_id": evaluation.job_id,
                    "candidate_id": evaluation.candidate_id,
                    "target_kind": evaluation.target_kind,
                    "target_id": evaluation.target_id,
                    "scenario_user_playbook_id": evaluation.scenario_user_playbook_id,
                    "source_interaction_ids": evaluation.source_interaction_ids,
                    "score": evaluation.score,
                    "verdict": evaluation.verdict,
                    "likert": evaluation.likert,
                    "rationale": evaluation.rationale,
                    "asi_json": _json_data(evaluation.asi_json),
                    "incumbent_rollout_json": _json_data(
                        evaluation.incumbent_rollout_json, default=[]
                    ),
                    "candidate_rollout_json": _json_data(
                        evaluation.candidate_rollout_json, default=[]
                    ),
                    "created_at": evaluation.created_at,
                }
            )
            .execute()
        )
        rows = _rows(response)
        if rows:
            evaluation.evaluation_id = int(rows[0]["evaluation_id"])
        return evaluation

    @handle_exceptions
    def list_playbook_optimization_evaluations(
        self, job_id: int
    ) -> list[PlaybookOptimizationEvaluation]:
        response = (
            self._table("playbook_optimization_evaluations")
            .select("*")
            .eq("job_id", job_id)
            .order("evaluation_id", desc=False)
            .execute()
        )
        return [
            PlaybookOptimizationEvaluation(
                evaluation_id=int(row["evaluation_id"]),
                job_id=int(row["job_id"]),
                candidate_id=int(row["candidate_id"]),
                target_kind=row["target_kind"],
                target_id=int(row["target_id"]),
                scenario_user_playbook_id=row.get("scenario_user_playbook_id"),
                source_interaction_ids=row.get("source_interaction_ids") or [],
                score=float(row["score"]),
                verdict=row["verdict"],
                likert=int(row["likert"]),
                rationale=row.get("rationale") or "",
                asi_json=_json_text(row.get("asi_json")),
                incumbent_rollout_json=_json_text(
                    row.get("incumbent_rollout_json") or []
                ),
                candidate_rollout_json=_json_text(
                    row.get("candidate_rollout_json") or []
                ),
                created_at=int(row["created_at"]),
            )
            for row in _rows(response)
        ]

    @handle_exceptions
    def insert_playbook_optimization_event(
        self, event: PlaybookOptimizationEvent
    ) -> PlaybookOptimizationEvent:
        response = (
            self._table("playbook_optimization_events")
            .insert(
                {
                    "job_id": event.job_id,
                    "event_type": event.event_type,
                    "payload_json": _json_data(event.payload_json),
                    "created_at": event.created_at,
                }
            )
            .execute()
        )
        rows = _rows(response)
        if rows:
            event.event_id = int(rows[0]["event_id"])
        return event

    # ==============================
    # Agent Success Evaluation methods
    # ==============================

    @handle_exceptions
    def save_agent_success_evaluation_results(
        self, results: list[AgentSuccessEvaluationResult]
    ) -> None:
        """
        Save agent success evaluation results with embeddings.

        Args:
            results (list[AgentSuccessEvaluationResult]): List of agent success evaluation result objects to save
        """
        for result in results:
            # Generate embedding from combined content
            embedding_text = f"{result.failure_type} {result.failure_reason}"
            if embedding_text.strip():
                embedding = self._get_embedding(embedding_text)
                result.embedding = embedding
            else:
                result.embedding = []

            self._table("agent_success_evaluation_result").upsert(
                agent_success_evaluation_result_to_data(result)
            ).execute()

    @handle_exceptions
    def get_agent_success_evaluation_results(
        self, limit: int = 100, agent_version: str | None = None
    ) -> list[AgentSuccessEvaluationResult]:
        """
        Get agent success evaluation results from storage.

        Args:
            limit (int): Maximum number of results to return
            agent_version (str, optional): The agent version to filter by. If None, returns all results.

        Returns:
            list[AgentSuccessEvaluationResult]: List of agent success evaluation result objects
        """
        query = (
            self._table("agent_success_evaluation_result")
            .select(_EVAL_RESULT_COLUMNS)
            .order("created_at", desc=True)
            .limit(limit)
        )

        # Add agent_version filter if specified
        if agent_version is not None:
            query = query.eq("agent_version", agent_version)

        response = query.execute()
        return [
            AgentSuccessEvaluationResult(
                result_id=int(item["result_id"]),
                session_id=item["session_id"],
                agent_version=item["agent_version"],
                evaluation_name=item.get("evaluation_name"),
                is_success=item["is_success"],
                failure_type=item["failure_type"],
                failure_reason=item["failure_reason"],
                created_at=self._parse_datetime_to_timestamp(item["created_at"]),
                regular_vs_shadow=(
                    RegularVsShadow(item["regular_vs_shadow"])
                    if item.get("regular_vs_shadow")
                    else None
                ),
                number_of_correction_per_session=item.get(
                    "number_of_correction_per_session", 0
                )
                or 0,
                user_turns_to_resolution=item.get("user_turns_to_resolution"),
                is_escalated=item.get("is_escalated", False) or False,
                embedding=[],
            )
            for item in _rows(response)
        ]

    @handle_exceptions
    def delete_all_agent_success_evaluation_results(self) -> None:
        """Delete all agent success evaluation results from storage."""
        self._table("agent_success_evaluation_result").delete().gte(
            "result_id", 0
        ).execute()

    @handle_exceptions
    def delete_agent_success_evaluation_results_for_session(
        self,
        session_id: str,
        evaluation_name: str,
        agent_version: str,
    ) -> int:
        response = (
            self._table("agent_success_evaluation_result")
            .delete()
            .eq("session_id", session_id)
            .eq("evaluation_name", evaluation_name)
            .eq("agent_version", agent_version)
            .execute()
        )
        return len(_rows(response))

    @handle_exceptions
    def delete_agent_success_evaluation_results_by_ids(
        self, result_ids: list[int]
    ) -> int:
        if not result_ids:
            return 0
        response = (
            self._table("agent_success_evaluation_result")
            .delete()
            .in_("result_id", result_ids)
            .execute()
        )
        return len(_rows(response))


def _order_by_ids(items: list[Any], ids: Sequence[Any], id_attr: str) -> list[Any]:
    by_id = {str(getattr(item, id_attr)): item for item in items}
    return [by_id[str(item_id)] for item_id in ids if str(item_id) in by_id]
