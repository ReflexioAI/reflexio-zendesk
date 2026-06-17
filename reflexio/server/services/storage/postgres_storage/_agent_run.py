"""Postgres storage for extraction agent run records."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg2 import sql
from psycopg2.extras import Json, RealDictCursor

from reflexio.server.services.storage.storage_base import (
    AgentBinding,
    AgentRunRecord,
    AgentRunStatus,
    PendingToolCallRecord,
    PendingToolCallStatus,
    PendingToolCallUpsertResult,
    PriorAnswerMatch,
    RunToolDependencyKind,
    RunToolDependencyRecord,
    embedding_similarity,
    not_applicable_tool_result,
)

from ._base import PostgresStorageBase

handle_exceptions = PostgresStorageBase.handle_exceptions


def _dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _dt_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _record_to_prior_answer_match(
    record: PendingToolCallRecord,
    *,
    query_embedding: list[float] | None = None,
) -> PriorAnswerMatch:
    return PriorAnswerMatch(
        pending_tool_call_id=record.id,
        status=record.status,
        question_text=record.question_text,
        result=record.result,
        valid_until=record.valid_until,
        answer_format=record.answer_format,
        created_at=record.created_at,
        resolved_at=record.resolved_at,
        expires_at=record.expires_at,
        similarity=embedding_similarity(query_embedding, record.embedding),
    )


def _row_to_agent_run(row: dict[str, Any]) -> AgentRunRecord:
    binding = AgentBinding(
        org_id=row["org_id"],
        extractor_kind=row["extractor_kind"],
        user_id=row.get("user_id"),
        request_id=row["request_id"],
        agent_version=row.get("agent_version"),
        source=row.get("source"),
        source_interaction_ids=list(row.get("source_interaction_ids") or []),
        window_start_interaction_id=row.get("window_start_interaction_id"),
        window_end_interaction_id=row.get("window_end_interaction_id"),
        extractor_config_hash=row.get("extractor_config_hash"),
    )
    return AgentRunRecord(
        id=row["id"],
        binding=binding,
        status=AgentRunStatus(row["status"]),
        generation_request_snapshot=row.get("generation_request_snapshot") or {},
        service_config_snapshot=row.get("service_config_snapshot"),
        agent_context_snapshot=row.get("agent_context_snapshot"),
        committed_output=row.get("committed_output"),
        pending_tool_call_ids=list(row.get("pending_tool_call_ids") or []),
        max_steps_remaining=row.get("max_steps_remaining"),
        resume_attempts=int(row.get("resume_attempts") or 0),
        finalization_attempts=int(row.get("finalization_attempts") or 0),
        next_resume_at=_dt(row.get("next_resume_at")),
        claimed_by=row.get("claimed_by"),
        claimed_at=_dt(row.get("claimed_at")),
        agent_completed_at=_dt(row.get("agent_completed_at")),
        finalized_at=_dt(row.get("finalized_at")),
        created_at=_dt(row.get("created_at")),
        updated_at=_dt(row.get("updated_at")),
        expires_at=_dt(row.get("expires_at")),
        last_error=row.get("last_error"),
    )


def _row_to_pending_tool_call(row: dict[str, Any]) -> PendingToolCallRecord:
    return PendingToolCallRecord(
        id=row["id"],
        org_id=row["org_id"],
        scope=row.get("scope") or {},
        scope_hash=row["scope_hash"],
        tool_name=row["tool_name"],
        dedup_key=row["dedup_key"],
        status=PendingToolCallStatus(row["status"]),
        question_text=row["question_text"],
        args=row.get("args") or {},
        tags=list(row.get("tags") or []),
        user_id=row.get("user_id"),
        answer_format=row.get("answer_format"),
        result=row.get("result"),
        embedding=row.get("embedding"),
        superseded_by=row.get("superseded_by"),
        created_at=_dt(row.get("created_at")),
        resolved_at=_dt(row.get("resolved_at")),
        expires_at=_dt(row.get("expires_at")),
        cache_until=_dt(row.get("cache_until")),
        valid_until=_dt(row.get("valid_until")),
    )


def _row_to_run_tool_dependency(row: dict[str, Any]) -> RunToolDependencyRecord:
    return RunToolDependencyRecord(
        run_id=row["run_id"],
        pending_tool_call_id=row["pending_tool_call_id"],
        dependency_kind=RunToolDependencyKind(row["dependency_kind"]),
        resolved_at=_dt(row.get("resolved_at")),
        consumed_at=_dt(row.get("consumed_at")),
        created_at=_dt(row.get("created_at")),
    )


class PostgresAgentRunMixin:
    """Postgres-backed extraction run storage."""

    _fetch_all: Any
    _table_identifier: Any
    _current_timestamp: Any
    pool: Any
    org_id: str
    schema_name: str

    def _fetch_agent_rows_tx(
        self,
        cursor: Any,
        query: sql.Composable,
        params: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        cursor.execute(
            sql.SQL("SET LOCAL search_path TO {}, public, extensions").format(
                sql.Identifier(self.schema_name)
            )
        )
        cursor.execute(query, params or [])
        if cursor.description is None:
            return []
        return [dict(row) for row in cursor.fetchall()]

    def _finalize_runs_without_pending_dependencies_tx(
        self, cursor: Any, now: datetime
    ) -> None:
        self._fetch_agent_rows_tx(
            cursor,
            sql.SQL(
                """
                UPDATE {} AS r
                SET status = %s,
                    finalized_at = COALESCE(finalized_at, %s),
                    updated_at = %s
                WHERE r.status = %s
                  AND NOT EXISTS (
                    SELECT 1
                    FROM {} AS d
                    JOIN {} AS p
                      ON p.id = d.pending_tool_call_id
                    WHERE d.run_id = r.id
                      AND d.resolved_at IS NULL
                      AND d.consumed_at IS NULL
                      AND p.status = %s
                  )
                """
            ).format(
                self._table_identifier("_agent_runs"),
                self._table_identifier("_run_tool_dependencies"),
                self._table_identifier("_pending_tool_calls"),
            ),
            [
                AgentRunStatus.FINALIZED.value,
                _dt_utc(now),
                _dt_utc(now),
                AgentRunStatus.FINALIZED_PENDING_TOOL.value,
                PendingToolCallStatus.PENDING.value,
            ],
        )

    def _mark_runs_ready_with_actionable_dependencies_tx(
        self, cursor: Any, now: datetime, *, pending_tool_call_id: str
    ) -> None:
        self._fetch_agent_rows_tx(
            cursor,
            sql.SQL(
                """
                UPDATE {} AS r
                SET status = %s,
                    updated_at = %s
                WHERE r.status IN (%s, %s)
                  AND EXISTS (
                    SELECT 1
                    FROM {} AS changed
                    WHERE changed.run_id = r.id
                      AND changed.pending_tool_call_id = %s
                  )
                  AND EXISTS (
                    SELECT 1
                    FROM {} AS d
                    JOIN {} AS p
                      ON p.id = d.pending_tool_call_id
                    WHERE d.run_id = r.id
                      AND d.resolved_at IS NOT NULL
                      AND d.consumed_at IS NULL
                      AND p.status = %s
                      AND NOT (p.result @> %s::jsonb)
                  )
                """
            ).format(
                self._table_identifier("_agent_runs"),
                self._table_identifier("_run_tool_dependencies"),
                self._table_identifier("_run_tool_dependencies"),
                self._table_identifier("_pending_tool_calls"),
            ),
            [
                AgentRunStatus.RESUME_READY.value,
                _dt_utc(now),
                AgentRunStatus.FINALIZED.value,
                AgentRunStatus.FINALIZED_PENDING_TOOL.value,
                pending_tool_call_id,
                PendingToolCallStatus.RESOLVED.value,
                Json(not_applicable_tool_result()),
            ],
        )

    def _finalize_runs_without_actionable_dependencies_tx(
        self, cursor: Any, now: datetime, *, pending_tool_call_id: str
    ) -> None:
        self._fetch_agent_rows_tx(
            cursor,
            sql.SQL(
                """
                UPDATE {} AS r
                SET status = %s,
                    finalized_at = COALESCE(finalized_at, %s),
                    updated_at = %s
                WHERE r.status IN (%s, %s)
                  AND EXISTS (
                    SELECT 1
                    FROM {} AS changed
                    WHERE changed.run_id = r.id
                      AND changed.pending_tool_call_id = %s
                  )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM {} AS d
                    JOIN {} AS p
                      ON p.id = d.pending_tool_call_id
                    WHERE d.run_id = r.id
                      AND d.resolved_at IS NULL
                      AND d.consumed_at IS NULL
                      AND p.status = %s
                  )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM {} AS d
                    JOIN {} AS p
                      ON p.id = d.pending_tool_call_id
                    WHERE d.run_id = r.id
                      AND d.resolved_at IS NOT NULL
                      AND d.consumed_at IS NULL
                      AND p.status = %s
                      AND NOT (p.result @> %s::jsonb)
                  )
                """
            ).format(
                self._table_identifier("_agent_runs"),
                self._table_identifier("_run_tool_dependencies"),
                self._table_identifier("_run_tool_dependencies"),
                self._table_identifier("_pending_tool_calls"),
                self._table_identifier("_run_tool_dependencies"),
                self._table_identifier("_pending_tool_calls"),
            ),
            [
                AgentRunStatus.FINALIZED.value,
                _dt_utc(now),
                _dt_utc(now),
                AgentRunStatus.FINALIZED_PENDING_TOOL.value,
                AgentRunStatus.RESUME_READY.value,
                pending_tool_call_id,
                PendingToolCallStatus.PENDING.value,
                PendingToolCallStatus.RESOLVED.value,
                Json(not_applicable_tool_result()),
            ],
        )

    @handle_exceptions
    def create_agent_run(self, record: AgentRunRecord) -> AgentRunRecord:
        binding = record.binding
        rows = self._fetch_all(
            sql.SQL(
                """
                INSERT INTO {} (
                    id, org_id, extractor_kind, user_id,
                    request_id, agent_version, source, source_interaction_ids,
                    window_start_interaction_id, window_end_interaction_id,
                    extractor_config_hash, status, generation_request_snapshot,
                    service_config_snapshot, agent_context_snapshot,
                    committed_output, pending_tool_call_ids, max_steps_remaining,
                    resume_attempts, finalization_attempts, next_resume_at,
                    claimed_by, claimed_at, agent_completed_at, finalized_at,
                    expires_at, last_error
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                RETURNING *
                """
            ).format(self._table_identifier("_agent_runs")),
            [
                record.id,
                binding.org_id,
                binding.extractor_kind,
                binding.user_id,
                binding.request_id,
                binding.agent_version,
                binding.source,
                binding.source_interaction_ids,
                binding.window_start_interaction_id,
                binding.window_end_interaction_id,
                binding.extractor_config_hash,
                record.status.value,
                Json(record.generation_request_snapshot),
                Json(record.service_config_snapshot),
                record.agent_context_snapshot,
                Json(record.committed_output),
                record.pending_tool_call_ids,
                record.max_steps_remaining,
                record.resume_attempts,
                record.finalization_attempts,
                _dt_utc(record.next_resume_at),
                record.claimed_by,
                _dt_utc(record.claimed_at),
                _dt_utc(record.agent_completed_at),
                _dt_utc(record.finalized_at),
                _dt_utc(record.expires_at),
                record.last_error,
            ],
        )
        return _row_to_agent_run(rows[0])

    @handle_exceptions
    def get_agent_run(self, run_id: str) -> AgentRunRecord | None:
        rows = self._fetch_all(
            sql.SQL("SELECT * FROM {} WHERE id = %s").format(
                self._table_identifier("_agent_runs")
            ),
            [run_id],
        )
        return _row_to_agent_run(rows[0]) if rows else None

    @handle_exceptions
    def update_agent_run_status(
        self,
        run_id: str,
        status: AgentRunStatus,
        *,
        committed_output: dict[str, Any] | None = None,
        pending_tool_call_ids: list[str] | None = None,
        max_steps_remaining: int | None = None,
        next_resume_at: datetime | None = None,
        last_error: str | None = None,
        increment_finalization_attempts: bool = False,
        expected_statuses: tuple[AgentRunStatus, ...] | None = None,
    ) -> AgentRunRecord | None:
        assignments = [sql.SQL("status = %s"), sql.SQL("updated_at = now()")]
        params: list[Any] = [status.value]
        if committed_output is not None:
            assignments.append(sql.SQL("committed_output = %s"))
            params.append(Json(committed_output))
        if pending_tool_call_ids is not None:
            assignments.append(sql.SQL("pending_tool_call_ids = %s"))
            params.append(pending_tool_call_ids)
        if max_steps_remaining is not None:
            assignments.append(sql.SQL("max_steps_remaining = %s"))
            params.append(max(0, max_steps_remaining))
        if next_resume_at is not None:
            assignments.append(sql.SQL("next_resume_at = %s"))
            params.append(_dt_utc(next_resume_at))
        if last_error is not None:
            assignments.append(sql.SQL("last_error = %s"))
            params.append(last_error)
        if increment_finalization_attempts:
            assignments.append(
                sql.SQL("finalization_attempts = finalization_attempts + 1")
            )
        if status == AgentRunStatus.AGENT_COMPLETED:
            assignments.append(sql.SQL("agent_completed_at = now()"))
        if status in (
            AgentRunStatus.FINALIZED,
            AgentRunStatus.FINALIZED_PENDING_TOOL,
        ):
            assignments.append(sql.SQL("finalized_at = COALESCE(finalized_at, now())"))

        params.append(run_id)
        status_filter = sql.SQL("")
        if expected_statuses:
            status_filter = sql.SQL(" AND status = ANY(%s)")
            params.append([expected.value for expected in expected_statuses])
        rows = self._fetch_all(
            sql.SQL("UPDATE {} SET {} WHERE id = %s{} RETURNING *").format(
                self._table_identifier("_agent_runs"),
                sql.SQL(", ").join(assignments),
                status_filter,
            ),
            params,
        )
        return _row_to_agent_run(rows[0]) if rows else self.get_agent_run(run_id)

    @handle_exceptions
    def fail_running_agent_runs_for_request(
        self,
        *,
        org_id: str,
        extractor_kind: str,
        user_id: str | None,
        request_id: str,
        last_error: str,
    ) -> int:
        rows = self._fetch_all(
            sql.SQL(
                """
                UPDATE {}
                SET status = %s,
                    updated_at = now(),
                    last_error = %s
                WHERE org_id = %s
                  AND extractor_kind = %s
                  AND user_id IS NOT DISTINCT FROM %s
                  AND request_id = %s
                  AND status = ANY(%s)
                RETURNING 1
                """
            ).format(self._table_identifier("_agent_runs")),
            [
                AgentRunStatus.FAILED.value,
                last_error,
                org_id,
                extractor_kind,
                user_id,
                request_id,
                [AgentRunStatus.RUNNING.value, AgentRunStatus.RESUMING.value],
            ],
        )
        return len(rows)

    def _insert_pending_tool_call_tx(
        self, cursor: Any, record: PendingToolCallRecord
    ) -> None:
        self._fetch_agent_rows_tx(
            cursor,
            sql.SQL(
                """
                INSERT INTO {} (
                    id, org_id, user_id, scope, scope_hash, tool_name, dedup_key,
                    status, question_text, answer_format, args, tags, result,
                    embedding, superseded_by, resolved_at, expires_at, cache_until,
                    valid_until
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                """
            ).format(self._table_identifier("_pending_tool_calls")),
            [
                record.id,
                record.org_id,
                record.user_id,
                Json(record.scope),
                record.scope_hash,
                record.tool_name,
                record.dedup_key,
                record.status.value,
                record.question_text,
                record.answer_format,
                Json(record.args),
                Json(record.tags),
                Json(record.result),
                Json(record.embedding),
                record.superseded_by,
                _dt_utc(record.resolved_at),
                _dt_utc(record.expires_at),
                _dt_utc(record.cache_until),
                _dt_utc(record.valid_until),
            ],
        )

    @handle_exceptions
    def create_pending_tool_call(
        self, record: PendingToolCallRecord
    ) -> PendingToolCallRecord:
        rows = self._fetch_all(
            sql.SQL(
                """
                INSERT INTO {} (
                    id, org_id, user_id, scope, scope_hash, tool_name, dedup_key,
                    status, question_text, answer_format, args, tags, result,
                    embedding, superseded_by, resolved_at, expires_at, cache_until,
                    valid_until
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                RETURNING *
                """
            ).format(self._table_identifier("_pending_tool_calls")),
            [
                record.id,
                record.org_id,
                record.user_id,
                Json(record.scope),
                record.scope_hash,
                record.tool_name,
                record.dedup_key,
                record.status.value,
                record.question_text,
                record.answer_format,
                Json(record.args),
                Json(record.tags),
                Json(record.result),
                Json(record.embedding),
                record.superseded_by,
                _dt_utc(record.resolved_at),
                _dt_utc(record.expires_at),
                _dt_utc(record.cache_until),
                _dt_utc(record.valid_until),
            ],
        )
        return _row_to_pending_tool_call(rows[0])

    @handle_exceptions
    def create_or_attach_pending_tool_call(
        self,
        *,
        record: PendingToolCallRecord,
        dependency: RunToolDependencyRecord,
        now: datetime | None = None,
    ) -> PendingToolCallUpsertResult:
        current = now or datetime.now(UTC)
        created = False
        pending_tool_call_id = record.id
        conn = self.pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                rows = self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        SELECT *
                        FROM {}
                        WHERE org_id = %s
                          AND scope_hash = %s
                          AND tool_name = %s
                          AND dedup_key = %s
                          AND status = %s
                          AND cache_until > %s
                        ORDER BY created_at ASC
                        LIMIT 1
                        FOR UPDATE
                        """
                    ).format(self._table_identifier("_pending_tool_calls")),
                    [
                        record.org_id,
                        record.scope_hash,
                        record.tool_name,
                        record.dedup_key,
                        PendingToolCallStatus.PENDING.value,
                        _dt_utc(current),
                    ],
                )
                pending_tool_call_id = rows[0]["id"] if rows else record.id
                if not rows:
                    self._insert_pending_tool_call_tx(cursor, record)
                    created = True
                self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        INSERT INTO {} (
                            run_id, pending_tool_call_id, dependency_kind,
                            resolved_at, consumed_at
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (run_id, pending_tool_call_id) DO NOTHING
                        """
                    ).format(self._table_identifier("_run_tool_dependencies")),
                    [
                        dependency.run_id,
                        pending_tool_call_id,
                        dependency.dependency_kind.value,
                        _dt_utc(dependency.resolved_at),
                        _dt_utc(dependency.consumed_at),
                    ],
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)

        stored = self.get_pending_tool_call(pending_tool_call_id)
        if stored is None:  # pragma: no cover
            raise RuntimeError("Failed to create or attach pending tool call")
        return PendingToolCallUpsertResult(pending_tool_call=stored, created=created)

    @handle_exceptions
    def get_pending_tool_call(self, call_id: str) -> PendingToolCallRecord | None:
        rows = self._fetch_all(
            sql.SQL("SELECT * FROM {} WHERE id = %s").format(
                self._table_identifier("_pending_tool_calls")
            ),
            [call_id],
        )
        return _row_to_pending_tool_call(rows[0]) if rows else None

    @handle_exceptions
    def list_pending_tool_calls(
        self,
        *,
        status: PendingToolCallStatus | None = None,
        limit: int = 100,
    ) -> list[PendingToolCallRecord]:
        bounded_limit = max(1, min(limit, 500))
        params: list[Any] = [self.org_id]
        status_clause = sql.SQL("")
        if status is not None:
            status_clause = sql.SQL("AND status = %s")
            params.append(status.value)
        params.append(bounded_limit)
        rows = self._fetch_all(
            sql.SQL(
                """
                SELECT *
                FROM {}
                WHERE org_id = %s
                  {}
                ORDER BY created_at DESC, id ASC
                LIMIT %s
                """
            ).format(self._table_identifier("_pending_tool_calls"), status_clause),
            params,
        )
        return [_row_to_pending_tool_call(row) for row in rows]

    @handle_exceptions
    def cancel_pending_tool_call(
        self,
        call_id: str,
        *,
        cancelled_at: datetime | None = None,
    ) -> PendingToolCallRecord | None:
        now = cancelled_at or datetime.now(UTC)
        conn = self.pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        UPDATE {}
                        SET status = %s
                        WHERE id = %s
                          AND status = %s
                        """
                    ).format(self._table_identifier("_pending_tool_calls")),
                    [
                        PendingToolCallStatus.CANCELLED.value,
                        call_id,
                        PendingToolCallStatus.PENDING.value,
                    ],
                )
                self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        UPDATE {}
                        SET resolved_at = %s
                        WHERE pending_tool_call_id = %s
                          AND resolved_at IS NULL
                        """
                    ).format(self._table_identifier("_run_tool_dependencies")),
                    [_dt_utc(now), call_id],
                )
                self._finalize_runs_without_pending_dependencies_tx(cursor, now)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
        return self.get_pending_tool_call(call_id)

    @handle_exceptions
    def expire_pending_tool_calls(
        self,
        *,
        now: datetime | None = None,
        limit: int = 100,
    ) -> int:
        current = now or datetime.now(UTC)
        bounded_limit = max(1, min(limit, 500))
        call_ids: list[str] = []
        conn = self.pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                rows = self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        SELECT id
                        FROM {}
                        WHERE status = %s
                          AND expires_at IS NOT NULL
                          AND expires_at <= %s
                        ORDER BY expires_at ASC, created_at ASC, id ASC
                        LIMIT %s
                        FOR UPDATE
                        """
                    ).format(self._table_identifier("_pending_tool_calls")),
                    [
                        PendingToolCallStatus.PENDING.value,
                        _dt_utc(current),
                        bounded_limit,
                    ],
                )
                call_ids = [str(row["id"]) for row in rows]
                if call_ids:
                    self._fetch_agent_rows_tx(
                        cursor,
                        sql.SQL(
                            """
                            UPDATE {}
                            SET status = %s
                            WHERE id = ANY(%s)
                              AND status = %s
                            """
                        ).format(self._table_identifier("_pending_tool_calls")),
                        [
                            PendingToolCallStatus.EXPIRED.value,
                            call_ids,
                            PendingToolCallStatus.PENDING.value,
                        ],
                    )
                    self._fetch_agent_rows_tx(
                        cursor,
                        sql.SQL(
                            """
                            UPDATE {}
                            SET resolved_at = %s
                            WHERE pending_tool_call_id = ANY(%s)
                              AND resolved_at IS NULL
                            """
                        ).format(self._table_identifier("_run_tool_dependencies")),
                        [_dt_utc(current), call_ids],
                    )
                    self._finalize_runs_without_pending_dependencies_tx(cursor, current)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
        return len(call_ids)

    @handle_exceptions
    def find_active_pending_tool_call(
        self,
        *,
        org_id: str,
        scope_hash: str,
        tool_name: str,
        dedup_key: str,
        now: datetime | None = None,
    ) -> PendingToolCallRecord | None:
        rows = self._fetch_all(
            sql.SQL(
                """
                SELECT *
                FROM {}
                WHERE org_id = %s
                  AND scope_hash = %s
                  AND tool_name = %s
                  AND dedup_key = %s
                  AND status = %s
                  AND cache_until > %s
                ORDER BY created_at ASC
                LIMIT 1
                """
            ).format(self._table_identifier("_pending_tool_calls")),
            [
                org_id,
                scope_hash,
                tool_name,
                dedup_key,
                PendingToolCallStatus.PENDING.value,
                _dt_utc(now or datetime.now(UTC)),
            ],
        )
        return _row_to_pending_tool_call(rows[0]) if rows else None

    @handle_exceptions
    def search_prior_tool_calls(
        self,
        *,
        org_id: str,
        scope_hash: str,
        tool_name: str,
        query_embedding: list[float] | None = None,
        now: datetime | None = None,
        limit: int = 8,
    ) -> list[PriorAnswerMatch]:
        current = now or datetime.now(UTC)
        bounded_limit = max(1, min(limit, 50))
        rows = self._fetch_all(
            sql.SQL(
                """
                SELECT *
                FROM {}
                WHERE org_id = %s
                  AND scope_hash = %s
                  AND tool_name = %s
                  AND (
                    (
                      status = %s
                      AND (expires_at IS NULL OR expires_at > %s)
                    )
                    OR (
                      status = %s
                      AND (valid_until IS NULL OR valid_until > %s)
                    )
                  )
                ORDER BY
                  CASE status WHEN %s THEN 0 ELSE 1 END,
                  COALESCE(resolved_at, created_at) DESC,
                  id ASC
                """
            ).format(self._table_identifier("_pending_tool_calls")),
            [
                org_id,
                scope_hash,
                tool_name,
                PendingToolCallStatus.PENDING.value,
                _dt_utc(current),
                PendingToolCallStatus.RESOLVED.value,
                _dt_utc(current),
                PendingToolCallStatus.RESOLVED.value,
            ],
        )
        seen_resolved_dedup_keys: set[str] = set()
        records: list[PendingToolCallRecord] = []
        for row in rows:
            record = _row_to_pending_tool_call(row)
            if record.status == PendingToolCallStatus.RESOLVED:
                if record.dedup_key in seen_resolved_dedup_keys:
                    continue
                seen_resolved_dedup_keys.add(record.dedup_key)
            records.append(record)
        matches = [
            _record_to_prior_answer_match(record, query_embedding=query_embedding)
            for record in records
        ]
        if query_embedding:
            matches.sort(
                key=lambda match: (
                    match.similarity is not None,
                    match.similarity or -1.0,
                    match.resolved_at
                    or match.created_at
                    or datetime.min.replace(tzinfo=UTC),
                ),
                reverse=True,
            )
        return matches[:bounded_limit]

    @handle_exceptions
    def attach_run_tool_dependency(
        self, record: RunToolDependencyRecord
    ) -> RunToolDependencyRecord:
        rows = self._fetch_all(
            sql.SQL(
                """
                INSERT INTO {} (
                    run_id, pending_tool_call_id, dependency_kind, resolved_at,
                    consumed_at
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (run_id, pending_tool_call_id) DO NOTHING
                RETURNING *
                """
            ).format(self._table_identifier("_run_tool_dependencies")),
            [
                record.run_id,
                record.pending_tool_call_id,
                record.dependency_kind.value,
                _dt_utc(record.resolved_at),
                _dt_utc(record.consumed_at),
            ],
        )
        if not rows:
            rows = self._fetch_all(
                sql.SQL(
                    """
                    SELECT *
                    FROM {}
                    WHERE run_id = %s
                      AND pending_tool_call_id = %s
                    """
                ).format(self._table_identifier("_run_tool_dependencies")),
                [record.run_id, record.pending_tool_call_id],
            )
        if not rows:  # pragma: no cover
            raise RuntimeError("Failed to attach run tool dependency")
        return _row_to_run_tool_dependency(rows[0])

    @handle_exceptions
    def count_unresolved_followup_dependencies(
        self,
        *,
        org_id: str,
        extractor_kind: str,
        tool_name: str,
    ) -> int:
        rows = self._fetch_all(
            sql.SQL(
                """
                SELECT COUNT(*) AS count
                FROM {} AS d
                JOIN {} AS r ON r.id = d.run_id
                JOIN {} AS p ON p.id = d.pending_tool_call_id
                WHERE r.org_id = %s
                  AND r.extractor_kind = %s
                  AND p.tool_name = %s
                  AND p.status = %s
                  AND d.resolved_at IS NULL
                  AND d.consumed_at IS NULL
                """
            ).format(
                self._table_identifier("_run_tool_dependencies"),
                self._table_identifier("_agent_runs"),
                self._table_identifier("_pending_tool_calls"),
            ),
            [
                org_id,
                extractor_kind,
                tool_name,
                PendingToolCallStatus.PENDING.value,
            ],
        )
        return int(rows[0]["count"]) if rows else 0

    @handle_exceptions
    def list_run_tool_dependencies(self, run_id: str) -> list[RunToolDependencyRecord]:
        rows = self._fetch_all(
            sql.SQL("SELECT * FROM {} WHERE run_id = %s").format(
                self._table_identifier("_run_tool_dependencies")
            ),
            [run_id],
        )
        return [_row_to_run_tool_dependency(row) for row in rows]

    @handle_exceptions
    def resolve_pending_tool_call(
        self,
        call_id: str,
        *,
        result: dict[str, Any],
        resolved_at: datetime | None = None,
        valid_for_seconds: int,
    ) -> PendingToolCallRecord | None:
        resolved = resolved_at or datetime.now(UTC)
        valid_until = resolved + timedelta(seconds=valid_for_seconds)
        conn = self.pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                rows = self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        UPDATE {}
                        SET status = %s,
                            result = %s,
                            resolved_at = %s,
                            valid_until = %s
                        WHERE id = %s
                          AND status = %s
                        RETURNING *
                        """
                    ).format(self._table_identifier("_pending_tool_calls")),
                    [
                        PendingToolCallStatus.RESOLVED.value,
                        Json(result),
                        _dt_utc(resolved),
                        _dt_utc(valid_until),
                        call_id,
                        PendingToolCallStatus.PENDING.value,
                    ],
                )
                if not rows:
                    conn.commit()
                    return self.get_pending_tool_call(call_id)
                self._supersede_prior_answers_tx(cursor, call_id, resolved)
                self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        UPDATE {}
                        SET resolved_at = %s
                        WHERE pending_tool_call_id = %s
                          AND resolved_at IS NULL
                        """
                    ).format(self._table_identifier("_run_tool_dependencies")),
                    [_dt_utc(resolved), call_id],
                )
                self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        UPDATE {} AS r
                        SET status = %s,
                            updated_at = now()
                        WHERE r.status IN (%s, %s)
                          AND EXISTS (
                            SELECT 1
                            FROM {} AS d
                            WHERE d.run_id = r.id
                              AND d.pending_tool_call_id = %s
                              AND d.resolved_at IS NOT NULL
                              AND d.consumed_at IS NULL
                          )
                        """
                    ).format(
                        self._table_identifier("_agent_runs"),
                        self._table_identifier("_run_tool_dependencies"),
                    ),
                    [
                        AgentRunStatus.RESUME_READY.value,
                        AgentRunStatus.FINALIZED.value,
                        AgentRunStatus.FINALIZED_PENDING_TOOL.value,
                        call_id,
                    ],
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
        return self.get_pending_tool_call(call_id)

    def _supersede_prior_answers_tx(
        self, cursor: Any, call_id: str, resolved: datetime
    ) -> None:
        self._fetch_agent_rows_tx(
            cursor,
            sql.SQL(
                """
                UPDATE {} AS prior
                SET status = %s,
                    superseded_by = %s
                FROM {} AS current
                WHERE prior.id != %s
                  AND prior.status = %s
                  AND (prior.valid_until IS NULL OR prior.valid_until > %s)
                  AND current.id = %s
                  AND prior.org_id = current.org_id
                  AND prior.scope_hash = current.scope_hash
                  AND prior.tool_name = current.tool_name
                  AND prior.dedup_key = current.dedup_key
                """
            ).format(
                self._table_identifier("_pending_tool_calls"),
                self._table_identifier("_pending_tool_calls"),
            ),
            [
                PendingToolCallStatus.SUPERSEDED.value,
                call_id,
                call_id,
                PendingToolCallStatus.RESOLVED.value,
                _dt_utc(resolved),
                call_id,
            ],
        )

    @handle_exceptions
    def update_resolved_pending_tool_call_result(
        self,
        call_id: str,
        *,
        result: dict[str, Any],
        resolved_at: datetime | None = None,
        valid_for_seconds: int,
    ) -> PendingToolCallRecord | None:
        resolved = resolved_at or datetime.now(UTC)
        valid_until = resolved + timedelta(seconds=valid_for_seconds)
        conn = self.pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                rows = self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        UPDATE {}
                        SET result = %s,
                            resolved_at = %s,
                            valid_until = %s
                        WHERE id = %s
                          AND status = %s
                        RETURNING *
                        """
                    ).format(self._table_identifier("_pending_tool_calls")),
                    [
                        Json(result),
                        _dt_utc(resolved),
                        _dt_utc(valid_until),
                        call_id,
                        PendingToolCallStatus.RESOLVED.value,
                    ],
                )
                if not rows:
                    conn.commit()
                    return self.get_pending_tool_call(call_id)
                self._supersede_prior_answers_tx(cursor, call_id, resolved)
                self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        UPDATE {}
                        SET resolved_at = %s,
                            consumed_at = NULL
                        WHERE pending_tool_call_id = %s
                        """
                    ).format(self._table_identifier("_run_tool_dependencies")),
                    [_dt_utc(resolved), call_id],
                )
                self._mark_runs_ready_with_actionable_dependencies_tx(
                    cursor, resolved, pending_tool_call_id=call_id
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
        return self.get_pending_tool_call(call_id)

    @handle_exceptions
    def mark_pending_tool_call_not_applicable(
        self,
        call_id: str,
        *,
        resolved_at: datetime | None = None,
        valid_for_seconds: int,
    ) -> PendingToolCallRecord | None:
        resolved = resolved_at or datetime.now(UTC)
        valid_until = resolved + timedelta(seconds=valid_for_seconds)
        conn = self.pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                rows = self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        UPDATE {}
                        SET status = %s,
                            result = %s,
                            resolved_at = %s,
                            valid_until = %s
                        WHERE id = %s
                          AND status = ANY(%s)
                        RETURNING *
                        """
                    ).format(self._table_identifier("_pending_tool_calls")),
                    [
                        PendingToolCallStatus.RESOLVED.value,
                        Json(not_applicable_tool_result()),
                        _dt_utc(resolved),
                        _dt_utc(valid_until),
                        call_id,
                        [
                            PendingToolCallStatus.PENDING.value,
                            PendingToolCallStatus.RESOLVED.value,
                        ],
                    ],
                )
                if not rows:
                    conn.commit()
                    return self.get_pending_tool_call(call_id)
                self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        UPDATE {}
                        SET resolved_at = COALESCE(resolved_at, %s),
                            consumed_at = %s
                        WHERE pending_tool_call_id = %s
                          AND consumed_at IS NULL
                        """
                    ).format(self._table_identifier("_run_tool_dependencies")),
                    [_dt_utc(resolved), _dt_utc(resolved), call_id],
                )
                self._mark_runs_ready_with_actionable_dependencies_tx(
                    cursor, resolved, pending_tool_call_id=call_id
                )
                self._finalize_runs_without_actionable_dependencies_tx(
                    cursor, resolved, pending_tool_call_id=call_id
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
        return self.get_pending_tool_call(call_id)

    @handle_exceptions
    def claim_ready_agent_run(
        self,
        *,
        org_id: str,
        worker_id: str,
        now: datetime | None = None,
        claim_ttl_seconds: int = 600,
    ) -> AgentRunRecord | None:
        current = now or datetime.now(UTC)
        stale_before = current - timedelta(seconds=claim_ttl_seconds)
        run_id: str | None = None
        conn = self.pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                rows = self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        SELECT r.*
                        FROM {} AS r
                        WHERE r.org_id = %s
                          AND (
                            r.status = %s
                            OR (r.status = %s AND r.claimed_at < %s)
                          )
                          AND (r.next_resume_at IS NULL OR r.next_resume_at <= %s)
                          AND EXISTS (
                            SELECT 1
                            FROM {} AS d
                            JOIN {} AS p
                              ON p.id = d.pending_tool_call_id
                            WHERE d.run_id = r.id
                              AND d.resolved_at IS NOT NULL
                              AND d.consumed_at IS NULL
                              AND p.status = %s
                              AND NOT (p.result @> %s::jsonb)
                          )
                        ORDER BY
                            r.org_id ASC,
                            r.extractor_kind ASC,
                            COALESCE(r.user_id, '') ASC,
                            COALESCE(r.window_start_interaction_id, 0) ASC,
                            r.updated_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                        """
                    ).format(
                        self._table_identifier("_agent_runs"),
                        self._table_identifier("_run_tool_dependencies"),
                        self._table_identifier("_pending_tool_calls"),
                    ),
                    [
                        org_id,
                        AgentRunStatus.RESUME_READY.value,
                        AgentRunStatus.RESUMING.value,
                        _dt_utc(stale_before),
                        _dt_utc(current),
                        PendingToolCallStatus.RESOLVED.value,
                        Json(not_applicable_tool_result()),
                    ],
                )
                if not rows:
                    conn.commit()
                    return None
                run_id = str(rows[0]["id"])
                self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        UPDATE {}
                        SET status = %s,
                            claimed_by = %s,
                            claimed_at = %s,
                            resume_attempts = resume_attempts + 1,
                            updated_at = now()
                        WHERE id = %s
                        """
                    ).format(self._table_identifier("_agent_runs")),
                    [
                        AgentRunStatus.RESUMING.value,
                        worker_id,
                        _dt_utc(current),
                        run_id,
                    ],
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
        return self.get_agent_run(run_id) if run_id is not None else None

    @handle_exceptions
    def claim_finalization_failed_agent_run(
        self,
        *,
        org_id: str,
        worker_id: str,
        now: datetime | None = None,
        claim_ttl_seconds: int = 600,
    ) -> AgentRunRecord | None:
        current = now or datetime.now(UTC)
        stale_before = current - timedelta(seconds=claim_ttl_seconds)
        run_id: str | None = None
        conn = self.pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                rows = self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        SELECT *
                        FROM {}
                        WHERE org_id = %s
                          AND (
                            status = %s
                            OR (status = %s AND claimed_at < %s)
                            OR (status = %s AND updated_at < %s)
                          )
                          AND committed_output IS NOT NULL
                          AND (next_resume_at IS NULL OR next_resume_at <= %s)
                        ORDER BY
                            org_id ASC,
                            extractor_kind ASC,
                            COALESCE(user_id, '') ASC,
                            COALESCE(window_start_interaction_id, 0) ASC,
                            updated_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                        """
                    ).format(self._table_identifier("_agent_runs")),
                    [
                        org_id,
                        AgentRunStatus.FINALIZATION_FAILED.value,
                        AgentRunStatus.FINALIZING.value,
                        _dt_utc(stale_before),
                        AgentRunStatus.AGENT_COMPLETED.value,
                        _dt_utc(stale_before),
                        _dt_utc(current),
                    ],
                )
                if not rows:
                    conn.commit()
                    return None
                run_id = str(rows[0]["id"])
                self._fetch_agent_rows_tx(
                    cursor,
                    sql.SQL(
                        """
                        UPDATE {}
                        SET status = %s,
                            claimed_by = %s,
                            claimed_at = %s,
                            updated_at = now()
                        WHERE id = %s
                        """
                    ).format(self._table_identifier("_agent_runs")),
                    [
                        AgentRunStatus.FINALIZING.value,
                        worker_id,
                        _dt_utc(current),
                        run_id,
                    ],
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
        return self.get_agent_run(run_id) if run_id is not None else None

    @handle_exceptions
    def consume_run_tool_dependencies(self, run_id: str) -> int:
        rows = self._fetch_all(
            sql.SQL(
                """
                UPDATE {}
                SET consumed_at = now()
                WHERE run_id = %s
                  AND resolved_at IS NOT NULL
                  AND consumed_at IS NULL
                RETURNING 1
                """
            ).format(self._table_identifier("_run_tool_dependencies")),
            [run_id],
        )
        return len(rows)

    @handle_exceptions
    def list_resumable_work_org_ids(
        self,
        *,
        now: datetime | None = None,
        limit: int = 1000,
    ) -> list[str]:
        current = now or datetime.now(UTC)
        bounded_limit = max(1, min(limit, 10_000))
        rows = self._fetch_all(
            sql.SQL(
                """
                SELECT DISTINCT org_id FROM (
                    SELECT r.org_id
                    FROM {} AS r
                    WHERE r.status IN (%s, %s)
                      AND EXISTS (
                        SELECT 1
                        FROM {} AS d
                        JOIN {} AS p
                          ON p.id = d.pending_tool_call_id
                        WHERE d.run_id = r.id
                          AND d.resolved_at IS NOT NULL
                          AND d.consumed_at IS NULL
                          AND p.status = %s
                          AND NOT (p.result @> %s::jsonb)
                      )
                    UNION
                    SELECT org_id FROM {}
                    WHERE status IN (%s, %s, %s)
                    UNION
                    SELECT org_id FROM {}
                    WHERE status = %s
                      AND expires_at IS NOT NULL
                      AND expires_at <= %s
                ) AS work
                ORDER BY org_id ASC
                LIMIT %s
                """
            ).format(
                self._table_identifier("_agent_runs"),
                self._table_identifier("_run_tool_dependencies"),
                self._table_identifier("_pending_tool_calls"),
                self._table_identifier("_agent_runs"),
                self._table_identifier("_pending_tool_calls"),
            ),
            [
                AgentRunStatus.RESUME_READY.value,
                AgentRunStatus.RESUMING.value,
                PendingToolCallStatus.RESOLVED.value,
                Json(not_applicable_tool_result()),
                AgentRunStatus.FINALIZATION_FAILED.value,
                AgentRunStatus.FINALIZING.value,
                AgentRunStatus.AGENT_COMPLETED.value,
                PendingToolCallStatus.PENDING.value,
                _dt_utc(current),
                bounded_limit,
            ],
        )
        return [str(row["org_id"]) for row in rows]
