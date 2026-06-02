"""Postgres storage for extraction agent run records."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from psycopg2 import sql
from psycopg2.extras import Json

from reflexio.server.services.storage.storage_base import (
    AgentBinding,
    AgentRunRecord,
    AgentRunStatus,
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


def _row_to_agent_run(row: dict[str, Any]) -> AgentRunRecord:
    binding = AgentBinding(
        org_id=row["org_id"],
        extractor_kind=row["extractor_kind"],
        extractor_name=row["extractor_name"],
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


class PostgresAgentRunMixin:
    """Postgres-backed extraction run storage."""

    _fetch_all: Any
    _table_identifier: Any

    @handle_exceptions
    def create_agent_run(self, record: AgentRunRecord) -> AgentRunRecord:
        binding = record.binding
        rows = self._fetch_all(
            sql.SQL(
                """
                INSERT INTO {} (
                    id, org_id, extractor_kind, extractor_name, user_id,
                    request_id, agent_version, source, source_interaction_ids,
                    window_start_interaction_id, window_end_interaction_id,
                    extractor_config_hash, status, generation_request_snapshot,
                    service_config_snapshot, agent_context_snapshot,
                    committed_output, pending_tool_call_ids, max_steps_remaining,
                    resume_attempts, finalization_attempts, next_resume_at,
                    claimed_by, claimed_at, agent_completed_at, finalized_at,
                    expires_at, last_error
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s
                )
                RETURNING *
                """
            ).format(self._table_identifier("_agent_runs")),
            [
                record.id,
                binding.org_id,
                binding.extractor_kind,
                binding.extractor_name,
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
        rows = self._fetch_all(
            sql.SQL("UPDATE {} SET {} WHERE id = %s RETURNING *").format(
                self._table_identifier("_agent_runs"),
                sql.SQL(", ").join(assignments),
            ),
            params,
        )
        return _row_to_agent_run(rows[0]) if rows else None
