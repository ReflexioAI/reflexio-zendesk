"""Native Postgres storage base using direct SQL."""

from __future__ import annotations

import functools
import logging
import threading
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

from psycopg2 import pool, sql

from reflexio.models.api_schema.service_schemas import (
    Interaction,
    Request,
    Status,
    ToolUsed,
    UserActionType,
)
from reflexio.models.config_schema import (
    EMBEDDING_DIMENSIONS,
    APIKeyConfig,
    LLMConfig,
    PostgresSearchBackend,
    SearchMode,
    StorageConfigPostgres,
)
from reflexio.server.llm.litellm_client import LiteLLMClient, LiteLLMConfig
from reflexio.server.llm.model_defaults import ModelRole, resolve_model_name
from reflexio.server.llm.providers.embedding_service_provider import (
    EmbeddingUnavailableError,
)
from reflexio.server.services.storage.error import (
    StorageError,
    require_non_empty_session_id,
)
from reflexio.server.services.storage.postgres_storage._migration_utils import (
    check_migration_needed as _check_migration_needed,
)
from reflexio.server.services.storage.postgres_storage._migration_utils import (
    execute_migration,
    execute_postgres_prerequisites,
)
from reflexio.server.services.storage.postgres_storage._opensearch import (
    PostgresOpenSearch,
    opensearch_config_from_env,
)
from reflexio.server.services.storage.postgres_storage._timestamp_utils import (
    _parse_iso_timestamp,
)
from reflexio.server.services.storage.retention import (
    RETENTION_CASCADES,
    RetentionTarget,
)
from reflexio.server.services.storage.retention_mixin import RetentionMixin
from reflexio.server.services.storage.storage_base import BaseStorage
from reflexio.server.site_var.site_var_manager import SiteVarManager

from ._query import PostgresQuery, PostgresRpc, execute_fetch_all

logger = logging.getLogger(__name__)

_THIS_DIR = Path(__file__).resolve().parent
_MIGRATION_LOCK = threading.Lock()
_MIGRATED_TARGETS: set[tuple[str, str]] = set()


def _rows(response: Any) -> list[dict[str, Any]]:
    """Return table/RPC rows from the query response facade."""
    return cast(list[dict[str, Any]], response.data)


_PROFILE_COLUMNS = "profile_id, user_id, content, last_modified_timestamp, generated_from_request_id, profile_time_to_live, expiration_timestamp, custom_features, created_at, source, status, extractor_names, source_span, notes, reader_angle"
_INTERACTION_COLUMNS = "interaction_id, user_id, content, request_id, created_at, role, user_action, user_action_description, interacted_image_url, shadow_content, expert_content, tools_used, citations"
_REQUEST_COLUMNS = "request_id, user_id, created_at, source, agent_version, session_id"
_USER_PLAYBOOK_COLUMNS = 'user_playbook_id, user_id, playbook_name, created_at, request_id, agent_version, content, "trigger", rationale, blocking_issue, status, source, source_interaction_ids, source_span, notes, reader_angle'
_USER_PLAYBOOK_COLUMNS_WITH_EMBEDDING = _USER_PLAYBOOK_COLUMNS + ", embedding"
_AGENT_PLAYBOOK_COLUMNS = 'agent_playbook_id, playbook_name, created_at, agent_version, content, "trigger", rationale, blocking_issue, playbook_status, playbook_metadata, status'
_EVAL_RESULT_COLUMNS = "result_id, session_id, agent_version, evaluation_name, is_success, failure_type, failure_reason, created_at, regular_vs_shadow, number_of_correction_per_session, user_turns_to_resolution, is_escalated"
_OPERATION_STATE_COLUMNS = "service_name, operation_state, updated_at"


def _timestamp_to_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).isoformat()


def _build_status_or_condition(status_list: list[Status | None]) -> str | None:
    has_none = None in status_list
    status_values = [
        s.value for s in status_list if s is not None and hasattr(s, "value")
    ]
    conditions: list[str] = []
    if has_none:
        conditions.append("status.is.null")
    conditions.extend(f"status.eq.{sv}" for sv in status_values)
    return ",".join(conditions) if conditions else None


def _apply_status_filter_to_query(
    query: Any,
    status_filter: Sequence[Status | None],
) -> Any:
    has_none = None in status_filter
    status_strings: list[str] = []
    for status in status_filter:
        if status is None:
            continue
        if isinstance(status, Status):
            if status.value is not None:
                status_strings.append(status.value)
            else:
                has_none = True
        elif isinstance(status, str):
            status_strings.append(status)

    if has_none and status_strings:
        query = query.or_(f"status.is.null,status.in.({','.join(status_strings)})")
    elif has_none:
        query = query.is_("status", "null")
    elif status_strings:
        query = query.in_("status", status_strings)

    return query


def _parse_rpc_row_to_request(row: dict[str, Any]) -> Request:
    return Request(
        request_id=row["request_id"],
        user_id=row["request_user_id"],
        created_at=int(datetime.fromisoformat(row["request_created_at"]).timestamp()),
        source=row.get("request_source") or "",
        agent_version=row.get("request_agent_version") or "",
        session_id=require_non_empty_session_id(row.get("session_id")),
    )


def _parse_rpc_row_to_interaction(row: dict[str, Any]) -> Interaction:
    tools_used: list[ToolUsed] = []
    tools_used_data = row.get("interaction_tools_used")
    if tools_used_data and isinstance(tools_used_data, list):
        tools_used = [ToolUsed(**t) for t in tools_used_data if isinstance(t, dict)]

    return Interaction(
        interaction_id=row["interaction_id"],
        user_id=row["interaction_user_id"],
        content=row["interaction_content"],
        request_id=row["interaction_request_id"],
        created_at=int(
            datetime.fromisoformat(row["interaction_created_at"]).timestamp()
        ),
        role=row.get("interaction_role") or "User",
        user_action=UserActionType(row["interaction_user_action"]),
        user_action_description=row["interaction_user_action_description"],
        interacted_image_url=row["interaction_interacted_image_url"],
        shadow_content=row.get("interaction_shadow_content") or "",
        expert_content=row.get("interaction_expert_content") or "",
        tools_used=tools_used,
    )


def _calculate_success_rate(eval_data: list[dict[str, Any]]) -> float:
    total = len(eval_data)
    if total == 0:
        return 0.0
    success_count = sum(1 for item in eval_data if item.get("is_success"))
    return success_count / total * 100


class PostgresStorageBase(RetentionMixin, BaseStorage):
    """Base storage implementation for direct PostgreSQL access."""

    supports_embedding: ClassVar[bool] = True
    _MAX_RETRIES = 3
    _RETRY_BACKOFF_BASE = 0.5

    @staticmethod
    def _is_transient_error(exc: Exception) -> bool:
        text = str(exc)
        return (
            "ConnectionReset" in text
            or "ConnectionRefused" in text
            or "connection already closed" in text
            or "server closed the connection unexpectedly" in text
        )

    @staticmethod
    def handle_exceptions(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Exception | None = None
            for attempt in range(PostgresStorageBase._MAX_RETRIES):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if (
                        PostgresStorageBase._is_transient_error(exc)
                        and attempt < PostgresStorageBase._MAX_RETRIES - 1
                    ):
                        wait = PostgresStorageBase._RETRY_BACKOFF_BASE * (2**attempt)
                        logger.warning(
                            "Transient error in %s (attempt %d/%d), retrying in %.1fs: %s",
                            func.__name__,
                            attempt + 1,
                            PostgresStorageBase._MAX_RETRIES,
                            wait,
                            exc,
                        )
                        last_exc = exc
                        time.sleep(wait)
                        continue
                    logger.exception("Error in %s: %s", func.__name__, exc)
                    msg = f"{type(exc).__name__}: {exc}"
                    raise StorageError(message=msg.replace("\n", " ")) from exc
            raise StorageError(
                message=f"Failed after {PostgresStorageBase._MAX_RETRIES} retries"
            ) from last_exc

        return wrapper

    def __init__(
        self,
        org_id: str,
        config: StorageConfigPostgres,
        api_key_config: APIKeyConfig | None = None,
        llm_config: LLMConfig | None = None,
        enable_document_expansion: bool = False,
    ) -> None:
        super().__init__(org_id)
        self.api_key_config = api_key_config
        self._enable_document_expansion = enable_document_expansion
        self.db_url = config.db_url
        self.schema_name = config.schema_name or "public"
        self.pool_size = max(1, config.pool_size)
        self.search_backend = config.search_backend
        self._interaction_columns = _INTERACTION_COLUMNS
        self._table_columns_cache: dict[str, set[str]] = {}
        self._opensearch: PostgresOpenSearch | None = None

        if not self.db_url:
            raise StorageError(f"Postgres Storage for org {org_id} missing db_url")

        try:
            self.pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=self.pool_size,
                dsn=self.db_url,
            )
        except Exception as e:
            raise StorageError(f"Postgres Storage failed to connect: {e}") from e

        self.supabase_settings = SiteVarManager().get_site_var("supabase_settings")
        if isinstance(self.supabase_settings, dict):
            self.search_mode = SearchMode(
                self.supabase_settings.get("search_mode", "hybrid")
            )
            self.vector_weight = float(self.supabase_settings.get("vector_weight", 1.0))
            self.fts_weight = float(self.supabase_settings.get("fts_weight", 1.0))
        else:
            self.search_mode = SearchMode.HYBRID
            self.vector_weight = 1.0
            self.fts_weight = 1.0

        self.model_setting = SiteVarManager().get_site_var("llm_model_setting")
        if not isinstance(self.model_setting, dict):
            raise ValueError("llm_model_setting must be a dict")

        self.embedding_model_name = resolve_model_name(
            ModelRole.EMBEDDING,
            site_var_value=self.model_setting.get("embedding_model_name"),
            config_override=llm_config.embedding_model_name if llm_config else None,
            api_key_config=self.api_key_config,
        )
        self.embedding_dimensions = EMBEDDING_DIMENSIONS
        litellm_config = LiteLLMConfig(
            model=self.embedding_model_name,
            temperature=0.0,
            api_key_config=self.api_key_config,
        )
        self.llm_client = LiteLLMClient(litellm_config)
        self._ensure_migrated()
        self._ensure_opensearch()

    def close(self) -> None:
        self.pool.closeall()

    def _ensure_migrated(self) -> None:
        target = (self.db_url, self.schema_name)
        if target in _MIGRATED_TARGETS:
            return
        with _MIGRATION_LOCK:
            if target in _MIGRATED_TARGETS:
                return
            self.migrate()
            _MIGRATED_TARGETS.add(target)

    def _ensure_opensearch(self) -> None:
        if self.search_backend != PostgresSearchBackend.OPENSEARCH:
            return
        config = opensearch_config_from_env()
        if config is None:
            raise StorageError(
                message=(
                    "REFLEXIO_OPENSEARCH_ENDPOINT is required when Postgres "
                    "search_backend is 'opensearch'"
                )
            )
        self._opensearch = PostgresOpenSearch(self, config)
        self._opensearch.ensure_indexes()
        if config.sync_on_startup:
            self._opensearch.sync_all()

    def _current_timestamp(self) -> str:
        return datetime.now(UTC).isoformat()

    def _parse_datetime_to_timestamp(self, datetime_str: str) -> int:
        if not datetime_str:
            return int(datetime.now(UTC).timestamp())
        try:
            return _parse_iso_timestamp(datetime_str)
        except ValueError:
            logger.warning("Could not parse datetime string: %s", datetime_str)
            return int(datetime.now(UTC).timestamp())

    def _table(self, name: str) -> PostgresQuery:
        return PostgresQuery(self, name)

    def _rpc(self, fn: str, params: dict[str, Any] | None = None) -> PostgresRpc:
        return PostgresRpc(self, fn, params)

    # PostgREST-style delete-all: filter on a sentinel that no real id would equal.
    # Mirrors PostgresStorageBase._delete_all_text_keyed so shared mixins can call
    # the same helper across both backends.
    _DELETE_ALL_TEXT_SENTINEL = "__delete_all_sentinel__"

    def _delete_all_text_keyed(self, table: str, key_column: str) -> None:
        """Delete every row from a table keyed on a text column."""
        self._table(table).delete().neq(
            key_column, PostgresStorageBase._DELETE_ALL_TEXT_SENTINEL
        ).execute()

    def _table_identifier(self, name: str) -> sql.Composable:
        return sql.Identifier(self.schema_name, name)

    def _function_identifier(self, name: str) -> sql.Composable:
        return sql.Identifier(self.schema_name, name)

    def _fetch_all(
        self, query: sql.Composable, params: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        conn = self.pool.getconn()
        try:
            rows = execute_fetch_all(conn, query, params or [], self.schema_name)
            conn.commit()
            return rows
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)

    def _table_columns(self, table_name: str) -> set[str]:
        cached = self._table_columns_cache.get(table_name)
        if cached is not None:
            return cached
        rows = self._fetch_all(
            sql.SQL(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """
            ),
            [self.schema_name, table_name],
        )
        columns = {str(row["column_name"]) for row in rows}
        self._table_columns_cache[table_name] = columns
        return columns

    # -- Retention hooks (see RetentionMixin) --

    def _retention_table_exists(self, table_name: str) -> bool:
        return bool(self._table_columns(table_name))

    def _retention_count_rows(self, target: RetentionTarget) -> int:
        rows = self._fetch_all(
            sql.SQL("SELECT count(*) AS count FROM {}").format(
                self._table_identifier(target.table_name)
            )
        )
        return int(rows[0]["count"]) if rows else 0

    def _retention_select_oldest_keys(
        self, target: RetentionTarget, count: int
    ) -> list[tuple[Any, ...]]:
        key_sql = sql.SQL(", ").join(
            sql.Identifier(column) for column in target.id_columns
        )
        # id-column tiebreaker keeps the select deterministic when many rows
        # share the same `order_column` value.
        rows = self._fetch_all(
            sql.SQL("SELECT {} FROM {} ORDER BY {} ASC, {} ASC LIMIT %s").format(
                key_sql,
                self._table_identifier(target.table_name),
                sql.Identifier(target.order_column),
                key_sql,
            ),
            [count],
        )
        return [tuple(row[column] for column in target.id_columns) for row in rows]

    def _retention_delete_dependencies(
        self, target: RetentionTarget, keys: list[tuple[Any, ...]]
    ) -> None:
        ids = [key[0] for key in keys]
        for cascade in RETENTION_CASCADES.get(target.name, ()):
            self._delete_rows_by_column(cascade.table_name, cascade.fk_column, ids)

    def _retention_delete_target_rows(
        self, target: RetentionTarget, keys: list[tuple[Any, ...]]
    ) -> None:
        if len(target.id_columns) == 1:
            self._delete_rows_by_column(
                target.table_name, target.id_columns[0], [key[0] for key in keys]
            )
            return
        clauses: list[sql.Composable] = []
        params: list[Any] = []
        for key in keys:
            clauses.append(
                sql.SQL("(")
                + sql.SQL(" AND ").join(
                    sql.SQL("{} = %s").format(sql.Identifier(column))
                    for column in target.id_columns
                )
                + sql.SQL(")")
            )
            params.extend(key)
        self._fetch_all(
            sql.SQL("DELETE FROM {} WHERE {} RETURNING 1").format(
                self._table_identifier(target.table_name),
                sql.SQL(" OR ").join(clauses),
            ),
            params,
        )

    def _delete_rows_by_column(
        self, table_name: str, column_name: str, values: list[Any]
    ) -> None:
        if not values or not self._retention_table_exists(table_name):
            return
        # Postgres ANY(%s) takes a single array parameter, so no IN-list
        # parameter limit applies — pass the full list in one shot. Use
        # RETURNING 1 (rather than *) to keep the response cheap; the
        # caller does not read the result.
        self._fetch_all(
            sql.SQL("DELETE FROM {} WHERE {} = ANY(%s) RETURNING 1").format(
                self._table_identifier(table_name),
                sql.Identifier(column_name),
            ),
            [values],
        )

    def check_migration_needed(self) -> bool:
        return _check_migration_needed(self.db_url, self.schema_name)

    def migrate(self) -> bool:
        migration_folder = _THIS_DIR / "migrations"
        if not migration_folder.is_dir():
            logger.error(
                "Postgres migration folder %s does not exist", migration_folder
            )
            return False
        prerequisites_ok, prerequisites_message = execute_postgres_prerequisites(
            self.db_url
        )
        if not prerequisites_ok:
            raise StorageError(
                message=f"Postgres prerequisite setup failed: {prerequisites_message}"
            )
        success, message = execute_migration(
            db_url=self.db_url,
            schema=self.schema_name,
            target_backend="postgres",
        )
        if not success:
            raise StorageError(message=f"Migration failed: {message}")
        logger.info("Postgres migration succeeded for org %s: %s", self.org_id, message)
        return True

    def _get_embedding(
        self, text: str, purpose: Literal["document", "query"] = "document"
    ) -> list[float]:
        prefix = "search_document: " if purpose == "document" else "search_query: "
        try:
            return self.llm_client.get_embedding(
                prefix + text, self.embedding_model_name, self.embedding_dimensions
            )
        except EmbeddingUnavailableError as exc:
            logger.warning(
                "Embedding unavailable for %s text; continuing without vector: %s",
                purpose,
                exc,
            )
            return []

    def _should_expand_documents(self) -> bool:
        return self._enable_document_expansion

    def _expand_document(self, content: str) -> str | None:
        if not content:
            return None
        try:
            from reflexio.server.prompt.prompt_manager import PromptManager
            from reflexio.server.services.pre_retrieval import DocumentExpander

            prompt_manager = PromptManager()
            expander = DocumentExpander(
                llm_client=self.llm_client,
                prompt_manager=prompt_manager,
            )
            expanded = expander.expand(content)
            return expanded.expanded_text or None
        except Exception:
            logger.debug("Document expansion failed", exc_info=True)
            return None
