"""Governance storage primitives for native Postgres."""

from __future__ import annotations

import time
from typing import Any, Literal, cast

from psycopg2 import sql
from psycopg2.extras import Json

from reflexio.models.api_schema.domain.governance import (
    AuditEvent,
    PurgeOperation,
    PurgeOperationTarget,
)
from reflexio.models.config_schema import GovernanceRetentionConfig
from reflexio.server.services.storage.postgres_storage._base import PostgresStorageBase

handle_exceptions = PostgresStorageBase.handle_exceptions


def _now() -> int:
    return int(time.time())


def _audit_event(row: dict[str, Any]) -> AuditEvent:
    return AuditEvent(
        org_id=str(row["org_id"]),
        actor_type=cast(Any, row["actor_type"]),
        actor_ref=row.get("actor_ref"),
        operation=cast(Any, row["operation"]),
        entity_type=cast(Any, row["entity_type"]),
        entity_id=row.get("entity_id"),
        subject_ref=row.get("subject_ref"),
        request_ref=str(row["request_ref"]),
        idempotency_key=row.get("idempotency_key"),
        status=cast(Any, row["status"]),
        detail=row.get("detail"),
        created_at=int(row["created_at"]),
    )


def _purge_operation(row: dict[str, Any]) -> PurgeOperation:
    return PurgeOperation(
        purge_id=str(row["purge_id"]),
        org_id=str(row["org_id"]),
        operation_type=cast(Any, row["operation_type"]),
        scope_type=cast(Any, row["scope_type"]),
        subject_ref=row.get("subject_ref"),
        request_ref=str(row["request_ref"]),
        idempotency_key=str(row["idempotency_key"]),
        status=cast(Any, row["status"]),
        error_code=row.get("error_code"),
        error_detail=row.get("error_detail"),
        created_at=int(row["created_at"]),
        updated_at=int(row["updated_at"]),
        completed_at=(
            int(row["completed_at"]) if row.get("completed_at") is not None else None
        ),
    )


def _purge_target(row: dict[str, Any]) -> PurgeOperationTarget:
    return PurgeOperationTarget(
        purge_id=str(row["purge_id"]),
        target_name=str(row["target_name"]),
        target_ref=str(row.get("target_ref") or ""),
        phase=str(row["phase"]),
        status=cast(Any, row["status"]),
        detail=row.get("detail"),
        deleted_count=int(row.get("deleted_count") or 0),
        error_detail=row.get("error_detail"),
        started_at=int(row["started_at"]) if row.get("started_at") is not None else None,
        completed_at=(
            int(row["completed_at"]) if row.get("completed_at") is not None else None
        ),
    )


class PostgresGovernanceMixin:
    """Postgres-backed audit and purge tracking."""

    org_id: str
    _fetch_all: Any
    _table_identifier: Any
    _table: Any
    clear_user_data: Any
    _opensearch: Any

    @handle_exceptions
    def append_audit_event(self, event: AuditEvent) -> bool:
        if event.org_id != self.org_id:
            raise ValueError("Audit event org_id must match storage org_id")
        rows = self._fetch_all(
            sql.SQL(
                """
                INSERT INTO {} (
                    org_id, actor_type, actor_ref, operation, entity_type,
                    entity_id, subject_ref, request_ref, idempotency_key, status,
                    detail, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (org_id, idempotency_key)
                WHERE idempotency_key IS NOT NULL
                DO NOTHING
                RETURNING event_id
                """
            ).format(self._table_identifier("audit_events")),
            [
                event.org_id,
                event.actor_type,
                event.actor_ref,
                event.operation,
                event.entity_type,
                event.entity_id,
                event.subject_ref,
                event.request_ref,
                event.idempotency_key,
                event.status,
                Json(event.detail),
                event.created_at,
            ],
        )
        return bool(rows)

    @handle_exceptions
    def list_audit_events(
        self, subject_ref: str | None = None, *, org_id: str | None = None
    ) -> list[AuditEvent]:
        effective_org_id = org_id or self.org_id
        clauses: list[sql.Composable] = [sql.SQL("org_id = %s")]
        params: list[Any] = [effective_org_id]
        if subject_ref is not None:
            clauses.append(sql.SQL("subject_ref = %s"))
            params.append(subject_ref)
        rows = self._fetch_all(
            sql.SQL("SELECT * FROM {} WHERE {} ORDER BY created_at, event_id").format(
                self._table_identifier("audit_events"),
                sql.SQL(" AND ").join(clauses),
            ),
            params,
        )
        return [_audit_event(row) for row in rows]

    @handle_exceptions
    def begin_purge_operation(
        self,
        purge_id: str,
        idempotency_key: str,
        operation_type: Literal["user_erasure", "org_purge"],
        scope_type: Literal["user", "org"],
        subject_ref: str | None,
        request_ref: str,
    ) -> PurgeOperation:
        now = _now()
        rows = self._fetch_all(
            sql.SQL(
                """
                INSERT INTO {} (
                    org_id, purge_id, operation_type, scope_type, subject_ref,
                    request_ref, idempotency_key, status, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', %s, %s)
                ON CONFLICT (org_id, idempotency_key)
                DO UPDATE SET updated_at = {}.updated_at
                RETURNING *
                """
            ).format(
                self._table_identifier("purge_operations"),
                self._table_identifier("purge_operations"),
            ),
            [
                self.org_id,
                purge_id,
                operation_type,
                scope_type,
                subject_ref,
                request_ref,
                idempotency_key,
                now,
                now,
            ],
        )
        return _purge_operation(rows[0])

    @handle_exceptions
    def record_purge_target(
        self,
        purge_id: str,
        target_name: str,
        phase: str,
        status: Literal["pending", "running", "failed", "complete"],
        target_ref: str = "",
        detail: dict[str, object] | None = None,
        deleted_count: int = 0,
        error_detail: str | None = None,
    ) -> None:
        now = _now()
        self._fetch_all(
            sql.SQL(
                """
                INSERT INTO {} (
                    org_id, purge_id, target_name, target_ref, phase, status,
                    detail, deleted_count, error_detail, started_at, completed_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
                ON CONFLICT (org_id, purge_id, target_name, target_ref, phase)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    detail = EXCLUDED.detail,
                    deleted_count = EXCLUDED.deleted_count,
                    error_detail = EXCLUDED.error_detail,
                    started_at = COALESCE({}.started_at, EXCLUDED.started_at),
                    completed_at = EXCLUDED.completed_at
                RETURNING 1
                """
            ).format(
                self._table_identifier("purge_operation_targets"),
                self._table_identifier("purge_operation_targets"),
            ),
            [
                self.org_id,
                purge_id,
                target_name,
                target_ref,
                phase,
                status,
                Json(detail),
                deleted_count,
                error_detail,
                now if status == "running" else None,
                now if status == "complete" else None,
            ],
        )

    @handle_exceptions
    def list_purge_targets(
        self, purge_id: str, phase: str | None = None
    ) -> list[PurgeOperationTarget]:
        clauses: list[sql.Composable] = [
            sql.SQL("org_id = %s"),
            sql.SQL("purge_id = %s"),
        ]
        params: list[Any] = [self.org_id, purge_id]
        if phase is not None:
            clauses.append(sql.SQL("phase = %s"))
            params.append(phase)
        rows = self._fetch_all(
            sql.SQL(
                "SELECT * FROM {} WHERE {} ORDER BY phase, target_name, target_ref"
            ).format(
                self._table_identifier("purge_operation_targets"),
                sql.SQL(" AND ").join(clauses),
            ),
            params,
        )
        return [_purge_target(row) for row in rows]

    @handle_exceptions
    def purge_targets_prepared(self, purge_id: str) -> bool:
        rows = self._fetch_all(
            sql.SQL(
                """
                SELECT 1 FROM {}
                WHERE org_id = %s AND purge_id = %s
                  AND target_name = 'target_snapshot'
                  AND target_ref = 'all'
                  AND phase = 'prepare_targets'
                  AND status = 'complete'
                LIMIT 1
                """
            ).format(self._table_identifier("purge_operation_targets")),
            [self.org_id, purge_id],
        )
        return bool(rows)

    @handle_exceptions
    def prepare_governance_erase_targets(
        self,
        purge_id: str,
        user_id: str,
        owned_user_playbook_ids: set[int] | None = None,
    ) -> None:
        if self.purge_targets_prepared(purge_id):
            return
        counts = {
            "request": self._count_where("requests", "user_id", user_id),
            "interaction": self._count_where("interactions", "user_id", user_id),
            "profile": self._count_where("profiles", "user_id", user_id),
            "user_playbook": self._count_where("user_playbooks", "user_id", user_id),
            "agent_success_evaluation_result": self._count_where(
                "agent_success_evaluation_result", "user_id", user_id
            ),
            "profile_purge": 0,
            "user_playbook_purge": 0,
        }
        for target_name, count in counts.items():
            self.record_purge_target(
                purge_id,
                target_name,
                "delete",
                "pending",
                target_ref="all",
                detail={"count": count},
            )
        self.record_purge_target(
            purge_id,
            "target_snapshot",
            "prepare_targets",
            "complete",
            target_ref="all",
            detail={
                "owned_user_playbook_ids": sorted(owned_user_playbook_ids or []),
                "affected_agent_playbook_ids": [],
            },
        )

    def _count_where(self, table: str, column: str, value: Any) -> int:
        rows = self._fetch_all(
            sql.SQL("SELECT count(*) AS count FROM {} WHERE {} = %s").format(
                self._table_identifier(table), sql.Identifier(column)
            ),
            [value],
        )
        return int(rows[0]["count"]) if rows else 0

    @handle_exceptions
    def hide_governance_agent_playbooks_for_rebuild(self, purge_id: str) -> list[int]:
        rows = self._fetch_all(
            sql.SQL(
                """
                SELECT target_ref FROM {}
                WHERE org_id = %s AND purge_id = %s
                  AND target_name = 'agent_playbook'
                  AND phase = 'rebuild_without_erased_sources'
                  AND target_ref != ''
                  AND status != 'complete'
                ORDER BY target_ref
                """
            ).format(self._table_identifier("purge_operation_targets")),
            [self.org_id, purge_id],
        )
        ids = [int(row["target_ref"]) for row in rows]
        if ids:
            self._fetch_all(
                sql.SQL(
                    "UPDATE {} SET status = 'archive_in_progress' "
                    "WHERE agent_playbook_id = ANY(%s) RETURNING 1"
                ).format(self._table_identifier("agent_playbooks")),
                [ids],
            )
            for agent_playbook_id in ids:
                self.record_purge_target(
                    purge_id,
                    "agent_playbook",
                    "hide_for_rebuild",
                    "complete",
                    target_ref=str(agent_playbook_id),
                )
        return ids

    @handle_exceptions
    def apply_governance_user_data_delete(
        self, purge_id: str, user_id: str
    ) -> dict[str, int]:
        counts = self.clear_user_data(user_id)
        target_names = {
            "interactions": "interaction",
            "user_playbooks": "user_playbook",
            "profiles": "profile",
            "requests": "request",
            "agent_success_evaluation_results": "agent_success_evaluation_result",
            "purged_profiles": "profile_purge",
            "purged_user_playbooks": "user_playbook_purge",
        }
        for key, value in counts.items():
            self.record_purge_target(
                purge_id,
                target_names.get(key, key),
                "delete",
                "complete",
                target_ref="all",
                detail={"count": int(value)},
                deleted_count=int(value),
            )
        return counts

    @handle_exceptions
    def apply_governance_agent_playbook_rebuild(
        self,
        purge_id: str,
        agent_playbook_id: int,
        remaining_source_windows: list[dict[str, object]],
        content: str | None,
        trigger: str | None,
        rationale: str | None,
        blocking_issue: dict[str, object] | None,
        expanded_terms: str | None,
        tags: list[str] | None,
    ) -> None:
        self._table("agent_playbooks").update(
            {
                "content": content or "",
                "trigger": trigger,
                "rationale": rationale,
                "blocking_issue": blocking_issue,
                "expanded_terms": expanded_terms,
                "tags": tags,
                "status": None,
            }
        ).eq("agent_playbook_id", agent_playbook_id).execute()
        self._table("agent_playbook_source_user_playbooks").delete().eq(
            "agent_playbook_id", agent_playbook_id
        ).execute()
        if remaining_source_windows:
            source_window_rows: list[dict[str, Any]] = []
            for window in remaining_source_windows:
                user_playbook_id = window.get("user_playbook_id")
                if user_playbook_id is None:
                    continue
                source_window_rows.append(
                    {
                        "agent_playbook_id": agent_playbook_id,
                        "user_playbook_id": int(cast(Any, user_playbook_id)),
                        "source_interaction_ids": window.get(
                            "source_interaction_ids", []
                        ),
                    }
                )
            self._table("agent_playbook_source_user_playbooks").insert(
                source_window_rows
            ).execute()
        self.record_purge_target(
            purge_id,
            "agent_playbook",
            "rebuild_without_erased_sources",
            "complete",
            target_ref=str(agent_playbook_id),
        )
        if self._opensearch:
            response = (
                self._table("agent_playbooks")
                .select("*")
                .eq("agent_playbook_id", agent_playbook_id)
                .execute()
            )
            self._opensearch.index_rows("agent_playbooks", response.data or [])

    @handle_exceptions
    def complete_purge_operation_with_audit(
        self, purge_id: str, audit_event: AuditEvent
    ) -> PurgeOperation:
        audit_event.idempotency_key = audit_event.idempotency_key or purge_id
        self.append_audit_event(audit_event)
        now = _now()
        rows = self._fetch_all(
            sql.SQL(
                """
                UPDATE {} SET status = 'complete', error_code = NULL,
                    error_detail = NULL, updated_at = %s, completed_at = %s
                WHERE org_id = %s AND purge_id = %s
                RETURNING *
                """
            ).format(self._table_identifier("purge_operations")),
            [now, now, self.org_id, purge_id],
        )
        if not rows:
            raise ValueError(f"Purge operation {purge_id!r} not found")
        return _purge_operation(rows[0])

    @handle_exceptions
    def fail_purge_operation(
        self, purge_id: str, error_code: str, error_detail: str
    ) -> PurgeOperation:
        now = _now()
        rows = self._fetch_all(
            sql.SQL(
                """
                UPDATE {} SET status = 'failed', error_code = %s,
                    error_detail = %s, updated_at = %s, completed_at = %s
                WHERE org_id = %s AND purge_id = %s
                RETURNING *
                """
            ).format(self._table_identifier("purge_operations")),
            [error_code, error_detail, now, now, self.org_id, purge_id],
        )
        if not rows:
            raise ValueError(f"Purge operation {purge_id!r} not found")
        return _purge_operation(rows[0])

    @handle_exceptions
    def get_purge_operation(self, purge_id: str) -> PurgeOperation:
        rows = self._fetch_all(
            sql.SQL("SELECT * FROM {} WHERE org_id = %s AND purge_id = %s").format(
                self._table_identifier("purge_operations")
            ),
            [self.org_id, purge_id],
        )
        if not rows:
            raise ValueError(f"Purge operation {purge_id!r} not found")
        return _purge_operation(rows[0])

    @handle_exceptions
    def gc_governance_retention(self, *, config: GovernanceRetentionConfig) -> int:
        if not config.audit_events_retention_enabled:
            return 0
        cutoff = _now() - config.audit_events_retention_days * 24 * 60 * 60
        rows = self._fetch_all(
            sql.SQL(
                """
                DELETE FROM {} WHERE event_id IN (
                    SELECT event_id FROM {}
                    WHERE org_id = %s AND created_at < %s
                    ORDER BY created_at, event_id
                    LIMIT %s
                )
                RETURNING 1
                """
            ).format(
                self._table_identifier("audit_events"),
                self._table_identifier("audit_events"),
            ),
            [self.org_id, cutoff, config.audit_events_delete_batch_limit],
        )
        return len(rows)
