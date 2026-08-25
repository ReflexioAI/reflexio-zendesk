"""Governance storage primitives for native Postgres."""

from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any, Literal, cast

from psycopg2 import sql
from psycopg2.extras import Json

from reflexio.models.api_schema.domain.governance import (
    AuditEvent,
    PurgeOperation,
    PurgeOperationTarget,
    SubjectWriteBarrier,
)
from reflexio.models.config_schema import GovernanceRetentionConfig
from reflexio.server.services.governance.config import (
    get_governance_ref_secret,
    governance_subject_ref,
)
from reflexio.server.services.storage.error import SubjectWriteBarrierError
from reflexio.server.services.storage.governance_claims import (
    PurgeExecutionClaim,
    validate_purge_execution_claim,
)
from reflexio.server.services.storage.governance_validation import (
    _CANONICAL_DELETE_TARGET_NAMES,
    _PREPARE_PHASE,
    _SNAPSHOT_TARGET_NAME,
    _canonicalize_audit_event_for_persistence,
    _is_successful_erase_event,
    _successful_erase_identity,
    _validate_governance_error_code,
    _validate_governance_error_detail,
    _validate_governance_prefixed_ref,
    _validate_governance_purge_id,
)
from reflexio.server.services.storage.postgres_storage._base import PostgresStorageBase
from reflexio.server.services.storage.storage_base.evaluation_state_keys import (
    GRADE_ON_DEMAND_CACHE_PREFIX,
    build_agent_success_marker_key,
    build_grade_on_demand_session_prefix,
)
from reflexio.server.services.storage.storage_base.retrieved_learning_state import (
    build_retrieved_learning_state_key,
)

from ._protocols import SchemaScopedClient

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
        started_at=int(row["started_at"])
        if row.get("started_at") is not None
        else None,
        completed_at=(
            int(row["completed_at"]) if row.get("completed_at") is not None else None
        ),
    )


def _subject_barrier(row: dict[str, Any]) -> SubjectWriteBarrier:
    return SubjectWriteBarrier(
        org_id=str(row["org_id"]),
        subject_ref=str(row["subject_ref"]),
        purge_id=str(row["purge_id"]),
        status=cast(Any, row["status"]),
        error_code=row.get("error_code"),
        error_detail=row.get("error_detail"),
        created_at=int(row["created_at"]),
        updated_at=int(row["updated_at"]),
    )


class PostgresGovernanceMixin(SchemaScopedClient):
    """Postgres-backed audit and purge tracking."""

    org_id: str
    _fetch_all: Any
    _table_identifier: Any
    _table: Any
    clear_user_data: Any
    _opensearch: Any
    commit_scope: Any

    def _subject_ref_for_user_id(self, user_id: str) -> str:
        return governance_subject_ref(self.org_id, user_id, get_governance_ref_secret())

    def _authoritative_user_digest(self, purge_id: str, user_id: str) -> str:
        material = f"authoritative-user-v1\0{self.org_id}\0{purge_id}\0{user_id}"
        return hmac.new(
            get_governance_ref_secret().encode(),
            material.encode(),
            hashlib.sha256,
        ).hexdigest()

    def _assert_authoritative_user_identity_locked(
        self, purge_id: str, user_id: str
    ) -> str:
        rows = self._fetch_all(
            sql.SQL(
                """
                SELECT operation_type, scope_type, subject_ref,
                       authoritative_user_digest
                FROM {}
                WHERE org_id = %s AND purge_id = %s
                FOR UPDATE
                """
            ).format(self._table_identifier("purge_operations")),
            [self.org_id, purge_id],
        )
        expected_digest = self._authoritative_user_digest(purge_id, user_id)
        if (
            not rows
            or rows[0]["operation_type"] != "user_erasure"
            or rows[0]["scope_type"] != "user"
            or rows[0]["subject_ref"] != self._subject_ref_for_user_id(user_id)
            or rows[0]["authoritative_user_digest"] != expected_digest
        ):
            raise ValueError("Purge authoritative user identity does not match")
        return expected_digest

    def _assert_purge_operation_execution_claim_locked(
        self,
        purge_id: str,
        execution_claim: PurgeExecutionClaim,
    ) -> None:
        claim = validate_purge_execution_claim(purge_id, execution_claim)
        now = _now()
        rows = self._fetch_all(
            sql.SQL(
                """
                SELECT status, execution_claim_owner, execution_claim_fence,
                       execution_claim_expires_at
                FROM {}
                WHERE org_id = %s AND purge_id = %s
                FOR UPDATE
                """
            ).format(self._table_identifier("purge_operations")),
            [self.org_id, purge_id],
        )
        if not rows:
            raise ValueError(f"Purge operation {purge_id!r} not found")
        row = rows[0]
        if (
            row["status"] != "running"
            or row.get("execution_claim_owner") != claim.owner
            or int(row.get("execution_claim_fence") or 0) != claim.fence
            or row.get("execution_claim_expires_at") is None
            or int(row["execution_claim_expires_at"]) <= now
        ):
            raise ValueError("purge execution claim is no longer active")

    def _active_subject_barrier(self, subject_ref: str) -> dict[str, Any] | None:
        rows = self._fetch_all(
            sql.SQL(
                """SELECT * FROM {} WHERE org_id = %s AND subject_ref = %s
                   AND status IN ('erasing', 'erased')"""
            ).format(self._table_identifier("subject_write_barriers")),
            [self.org_id, subject_ref],
        )
        return rows[0] if rows else None

    def _assert_subject_writable_locked(self, subject_ref: str) -> None:
        row = self._active_subject_barrier(subject_ref)
        if row is not None:
            raise SubjectWriteBarrierError(
                f"subject {subject_ref} is blocked by erasure barrier {row['purge_id']}"
            )

    def _same_subject_rows_remain(self, subject_ref: str) -> bool:
        tables = (
            "requests",
            "interactions",
            "profiles",
            "user_playbooks",
            "agent_success_evaluation_result",
            "retrieved_learning_evaluation",
            "session_outcomes",
        )
        for table in tables:
            rows = self._fetch_all(
                sql.SQL(
                    "SELECT 1 FROM {} WHERE governance_subject_ref = %s LIMIT 1"
                ).format(self._table_identifier(table)),
                [subject_ref],
            )
            if rows:
                return True

        # Rows written before governance_subject_ref was introduced must also
        # block completion. Recompute their minimized subject reference without
        # persisting or exposing the raw user id.
        for table in tables:
            columns = self._table_columns(table)
            if "user_id" not in columns:
                continue
            rows = self._fetch_all(
                sql.SQL(
                    "SELECT DISTINCT user_id FROM {} WHERE governance_subject_ref IS NULL"
                ).format(self._table_identifier(table))
            )
            if any(
                self._subject_ref_for_user_id(str(row["user_id"])) == subject_ref
                for row in rows
            ):
                return True
        return False

    @handle_exceptions
    def begin_subject_erasure_barrier(
        self,
        subject_ref: str,
        purge_id: str,
        *,
        execution_claim: PurgeExecutionClaim,
    ) -> SubjectWriteBarrier:
        _validate_governance_prefixed_ref(
            "subject_ref", subject_ref, prefix="subref_v1_"
        )
        validated_purge_id = _validate_governance_purge_id("purge_id", purge_id)
        now = _now()
        with self.commit_scope():
            self._assert_purge_operation_execution_claim_locked(
                validated_purge_id, execution_claim
            )
            purge_rows = self._fetch_all(
                sql.SQL(
                    "SELECT * FROM {} WHERE org_id = %s AND purge_id = %s FOR UPDATE"
                ).format(self._table_identifier("purge_operations")),
                [self.org_id, validated_purge_id],
            )
            if not purge_rows:
                raise ValueError(f"Purge operation {validated_purge_id!r} not found")
            purge = _purge_operation(purge_rows[0])
            if purge.subject_ref != subject_ref:
                raise ValueError(
                    "Purge operation subject_ref must match the barrier subject_ref"
                )
            existing = self._fetch_all(
                sql.SQL(
                    "SELECT * FROM {} WHERE org_id = %s AND subject_ref = %s FOR UPDATE"
                ).format(self._table_identifier("subject_write_barriers")),
                [self.org_id, subject_ref],
            )
            if existing and str(existing[0]["purge_id"]) != validated_purge_id:
                raise ValueError(
                    "Existing barrier purge_id must match the requested purge_id"
                )
            if existing and str(existing[0]["status"]) == "erased":
                return _subject_barrier(existing[0])
            rows = self._fetch_all(
                sql.SQL(
                    """INSERT INTO {} (
                           org_id, subject_ref, purge_id, status, created_at, updated_at
                       ) VALUES (%s, %s, %s, 'erasing', %s, %s)
                       ON CONFLICT (org_id, subject_ref) DO UPDATE SET
                           purge_id = EXCLUDED.purge_id, status = 'erasing',
                           error_code = NULL, error_detail = NULL,
                           updated_at = EXCLUDED.updated_at
                       RETURNING *"""
                ).format(self._table_identifier("subject_write_barriers")),
                [self.org_id, subject_ref, validated_purge_id, now, now],
            )
        return _subject_barrier(rows[0])

    @handle_exceptions
    def assert_subject_writable(self, subject_ref: str) -> None:
        _validate_governance_prefixed_ref(
            "subject_ref", subject_ref, prefix="subref_v1_"
        )
        self._assert_subject_writable_locked(subject_ref)

    @handle_exceptions
    def complete_subject_erasure_barrier_after_empty_check(
        self,
        purge_id: str,
        audit_event: AuditEvent,
        *,
        authoritative_user_id: str,
        execution_claim: PurgeExecutionClaim,
    ) -> PurgeOperation:
        validated_purge_id = _validate_governance_purge_id("purge_id", purge_id)
        if audit_event.org_id != self.org_id:
            raise ValueError("Audit event org_id must match storage org_id")
        if audit_event.idempotency_key != validated_purge_id:
            raise ValueError("Audit event idempotency key must match purge_id")
        if not _is_successful_erase_event(audit_event, purge_id=validated_purge_id):
            raise ValueError(
                "Completion requires a successful ERASE audit event for this purge"
            )
        audit_event = _canonicalize_audit_event_for_persistence(audit_event)
        now = _now()
        with self.commit_scope():
            self._assert_purge_operation_execution_claim_locked(
                validated_purge_id, execution_claim
            )
            self._assert_authoritative_user_identity_locked(
                validated_purge_id, authoritative_user_id
            )
            purge_rows = self._fetch_all(
                sql.SQL(
                    "SELECT * FROM {} WHERE org_id = %s AND purge_id = %s FOR UPDATE"
                ).format(self._table_identifier("purge_operations")),
                [self.org_id, validated_purge_id],
            )
            if not purge_rows:
                raise ValueError(f"Purge operation {validated_purge_id!r} not found")
            purge = _purge_operation(purge_rows[0])
            if purge.subject_ref != audit_event.subject_ref:
                raise ValueError(
                    "Audit event subject_ref must match purge operation subject_ref"
                )
            if purge.request_ref != audit_event.request_ref:
                raise ValueError(
                    "Audit event request_ref must match purge operation request_ref"
                )
            snapshot = self._fetch_all(
                sql.SQL(
                    """SELECT 1 FROM {} WHERE org_id = %s AND purge_id = %s
                       AND target_name = %s AND target_ref = 'all'
                       AND phase = %s AND status = 'complete'"""
                ).format(self._table_identifier("purge_operation_targets")),
                [
                    self.org_id,
                    validated_purge_id,
                    _SNAPSHOT_TARGET_NAME,
                    _PREPARE_PHASE,
                ],
            )
            if not snapshot:
                raise ValueError("Cannot complete purge without target snapshot marker")
            if self._same_subject_rows_remain(audit_event.subject_ref or ""):
                raise ValueError("same-subject rows remain")
            delete_rows = self._fetch_all(
                sql.SQL(
                    """SELECT target_name, status FROM {}
                       WHERE org_id = %s AND purge_id = %s AND phase = 'delete'
                       AND target_ref = 'all'"""
                ).format(self._table_identifier("purge_operation_targets")),
                [self.org_id, validated_purge_id],
            )
            delete_statuses = {
                str(row["target_name"]): str(row["status"]) for row in delete_rows
            }
            missing = [
                name
                for name in _CANONICAL_DELETE_TARGET_NAMES
                if delete_statuses.get(name) != "complete"
            ]
            if missing:
                raise ValueError(
                    "Cannot complete purge without complete delete target matrix: "
                    + ", ".join(missing)
                )
            incomplete = self._fetch_all(
                sql.SQL(
                    """SELECT 1 FROM {} WHERE org_id = %s AND purge_id = %s
                       AND status != 'complete' LIMIT 1"""
                ).format(self._table_identifier("purge_operation_targets")),
                [self.org_id, validated_purge_id],
            )
            if incomplete:
                raise ValueError("Cannot complete purge with incomplete targets")

            existing_audit = self._fetch_all(
                sql.SQL(
                    "SELECT * FROM {} WHERE org_id = %s AND idempotency_key = %s"
                ).format(self._table_identifier("audit_events")),
                [self.org_id, validated_purge_id],
            )
            if existing_audit:
                existing_event = _audit_event(existing_audit[0])
                if not _is_successful_erase_event(
                    existing_event, purge_id=validated_purge_id
                ) or _successful_erase_identity(
                    existing_event
                ) != _successful_erase_identity(audit_event):
                    raise ValueError(
                        "Existing audit row for purge_id must be the matching successful ERASE row"
                    )
            elif not self.append_audit_event(audit_event):
                raise ValueError("Completion requires a successful ERASE audit row")

            barrier = self._fetch_all(
                sql.SQL(
                    """UPDATE {} SET status = 'erased', error_code = NULL,
                           error_detail = NULL, updated_at = %s
                       WHERE org_id = %s AND subject_ref = %s AND purge_id = %s
                         AND status = 'erasing' RETURNING 1"""
                ).format(self._table_identifier("subject_write_barriers")),
                [
                    now,
                    self.org_id,
                    audit_event.subject_ref,
                    validated_purge_id,
                ],
            )
            if len(barrier) != 1:
                raise ValueError("subject erasure barrier is missing")
            completed = self._fetch_all(
                sql.SQL(
                    """UPDATE {} SET status = 'complete', error_code = NULL,
                           error_detail = NULL, updated_at = %s, completed_at = %s,
                           execution_claim_owner = NULL,
                           execution_claim_expires_at = NULL
                       WHERE org_id = %s AND purge_id = %s RETURNING *"""
                ).format(self._table_identifier("purge_operations")),
                [now, now, self.org_id, validated_purge_id],
            )
        return _purge_operation(completed[0])

    @handle_exceptions
    def fail_subject_erasure_barrier(
        self,
        subject_ref: str,
        purge_id: str,
        error_code: str,
        error_detail: str,
        *,
        execution_claim: PurgeExecutionClaim,
    ) -> SubjectWriteBarrier:
        _validate_governance_prefixed_ref(
            "subject_ref", subject_ref, prefix="subref_v1_"
        )
        validated_purge_id = _validate_governance_purge_id("purge_id", purge_id)
        code = _validate_governance_error_code(error_code)
        detail = _validate_governance_error_detail(error_detail)
        now = _now()
        with self.commit_scope():
            self._assert_purge_operation_execution_claim_locked(
                validated_purge_id, execution_claim
            )
            rows = self._fetch_all(
                sql.SQL(
                    """UPDATE {} SET status = 'failed', error_code = %s,
                           error_detail = %s, updated_at = %s
                       WHERE org_id = %s AND subject_ref = %s AND purge_id = %s
                         AND status = 'erasing' RETURNING *"""
                ).format(self._table_identifier("subject_write_barriers")),
                [code, detail, now, self.org_id, subject_ref, validated_purge_id],
            )
            if len(rows) != 1:
                raise ValueError(
                    "subject erasure barrier failure requires a matching barrier"
                )
            self._fetch_all(
                sql.SQL(
                    """UPDATE {} SET status = 'failed', error_code = %s,
                           error_detail = %s, updated_at = %s, completed_at = %s,
                           execution_claim_owner = NULL,
                           execution_claim_expires_at = NULL
                       WHERE org_id = %s AND purge_id = %s RETURNING 1"""
                ).format(self._table_identifier("purge_operations")),
                [code, detail, now, now, self.org_id, validated_purge_id],
            )
        return _subject_barrier(rows[0])

    @handle_exceptions
    def get_subject_write_barrier(self, subject_ref: str) -> SubjectWriteBarrier | None:
        _validate_governance_prefixed_ref(
            "subject_ref", subject_ref, prefix="subref_v1_"
        )
        rows = self._fetch_all(
            sql.SQL("SELECT * FROM {} WHERE org_id = %s AND subject_ref = %s").format(
                self._table_identifier("subject_write_barriers")
            ),
            [self.org_id, subject_ref],
        )
        return _subject_barrier(rows[0]) if rows else None

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
        authoritative_user_id: str | None = None,
    ) -> PurgeOperation:
        if operation_type == "user_erasure" and scope_type == "user":
            if not authoritative_user_id:
                raise ValueError("authoritative user identity is required")
            if subject_ref != self._subject_ref_for_user_id(authoritative_user_id):
                raise ValueError("authoritative user identity must match subject_ref")
        elif authoritative_user_id:
            raise ValueError(
                "authoritative user identity is only valid for user erasure"
            )
        authoritative_user_digest = (
            self._authoritative_user_digest(purge_id, authoritative_user_id)
            if authoritative_user_id
            else None
        )
        now = _now()
        with self.commit_scope():
            existing = self._fetch_all(
                sql.SQL(
                    "SELECT * FROM {} WHERE org_id = %s AND idempotency_key = %s FOR UPDATE"
                ).format(self._table_identifier("purge_operations")),
                [self.org_id, idempotency_key],
            )
            if existing:
                operation = _purge_operation(existing[0])
                expected = {
                    "purge_id": purge_id,
                    "operation_type": operation_type,
                    "scope_type": scope_type,
                    "subject_ref": subject_ref,
                    "request_ref": request_ref,
                }
                if (
                    any(
                        getattr(operation, name) != value
                        for name, value in expected.items()
                    )
                    or existing[0].get("authoritative_user_digest")
                    != authoritative_user_digest
                ):
                    raise ValueError(
                        "Existing purge operation for idempotency_key has mismatched identity"
                    )
                return operation
            rows = self._fetch_all(
                sql.SQL(
                    """
                    INSERT INTO {} (
                        org_id, purge_id, operation_type, scope_type, subject_ref,
                        request_ref, idempotency_key, authoritative_user_digest,
                        status, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s, %s)
                    RETURNING *
                    """
                ).format(self._table_identifier("purge_operations")),
                [
                    self.org_id,
                    purge_id,
                    operation_type,
                    scope_type,
                    subject_ref,
                    request_ref,
                    idempotency_key,
                    authoritative_user_digest,
                    now,
                    now,
                ],
            )
        return _purge_operation(rows[0])

    @handle_exceptions
    def claim_purge_operation_execution(
        self,
        purge_id: str,
        *,
        lease_owner: str,
        lease_ttl_seconds: int,
    ) -> PurgeExecutionClaim | None:
        purge_id = _validate_governance_purge_id("purge_id", purge_id)
        if not lease_owner.strip():
            raise ValueError("lease_owner is required")
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        now = _now()
        rows = self._fetch_all(
            sql.SQL(
                """
                UPDATE {}
                SET status = 'running', error_code = NULL, error_detail = NULL,
                    completed_at = NULL, updated_at = %s,
                    execution_claim_owner = %s,
                    execution_claim_fence = execution_claim_fence + 1,
                    execution_claim_expires_at = %s
                WHERE org_id = %s AND purge_id = %s
                  AND (
                    status IN ('pending', 'failed')
                    OR (
                        status = 'running'
                        AND (
                            execution_claim_expires_at IS NULL
                            OR execution_claim_expires_at <= %s
                        )
                    )
                  )
                RETURNING execution_claim_owner, execution_claim_fence,
                          execution_claim_expires_at
                """
            ).format(self._table_identifier("purge_operations")),
            [
                now,
                lease_owner,
                now + lease_ttl_seconds,
                self.org_id,
                purge_id,
                now,
            ],
        )
        if not rows:
            return None
        return PurgeExecutionClaim(
            purge_id=purge_id,
            owner=str(rows[0]["execution_claim_owner"]),
            fence=int(rows[0]["execution_claim_fence"]),
            expires_at=int(rows[0]["execution_claim_expires_at"]),
        )

    @handle_exceptions
    def assert_purge_operation_execution_claim(
        self, purge_id: str, execution_claim: PurgeExecutionClaim
    ) -> None:
        purge_id = _validate_governance_purge_id("purge_id", purge_id)
        with self.commit_scope():
            self._assert_purge_operation_execution_claim_locked(
                purge_id, execution_claim
            )

    @handle_exceptions
    def renew_purge_operation_execution_claim(
        self,
        purge_id: str,
        execution_claim: PurgeExecutionClaim,
        *,
        lease_ttl_seconds: int,
    ) -> PurgeExecutionClaim:
        purge_id = _validate_governance_purge_id("purge_id", purge_id)
        claim = validate_purge_execution_claim(purge_id, execution_claim)
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        now = _now()
        rows = self._fetch_all(
            sql.SQL(
                """
                UPDATE {}
                SET execution_claim_expires_at = %s, updated_at = %s
                WHERE org_id = %s AND purge_id = %s
                  AND status = 'running'
                  AND execution_claim_owner = %s
                  AND execution_claim_fence = %s
                  AND execution_claim_expires_at IS NOT NULL
                  AND execution_claim_expires_at > %s
                RETURNING execution_claim_owner, execution_claim_fence,
                          execution_claim_expires_at
                """
            ).format(self._table_identifier("purge_operations")),
            [
                now + lease_ttl_seconds,
                now,
                self.org_id,
                purge_id,
                claim.owner,
                claim.fence,
                now,
            ],
        )
        if not rows:
            raise ValueError("purge execution claim is no longer active")
        return PurgeExecutionClaim(
            purge_id=purge_id,
            owner=str(rows[0]["execution_claim_owner"]),
            fence=int(rows[0]["execution_claim_fence"]),
            expires_at=int(rows[0]["execution_claim_expires_at"]),
        )

    @handle_exceptions
    def record_purge_target(
        self,
        purge_id: str,
        target_name: str,
        phase: str,
        status: Literal["pending", "running", "failed", "complete"],
        *,
        execution_claim: PurgeExecutionClaim,
        target_ref: str = "",
        detail: dict[str, object] | None = None,
        deleted_count: int = 0,
        error_detail: str | None = None,
    ) -> None:
        now = _now()
        with self.commit_scope():
            self._assert_purge_operation_execution_claim_locked(
                purge_id, execution_claim
            )
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
                    now if status in {"running", "failed", "complete"} else None,
                    now if status in {"failed", "complete"} else None,
                ],
            )
            self._fetch_all(
                sql.SQL(
                    """
                    UPDATE {}
                    SET status = CASE
                        WHEN status IN ('complete', 'failed') THEN status
                        WHEN %s IN ('running', 'complete') THEN 'running'
                        ELSE status
                    END,
                    updated_at = %s
                    WHERE org_id = %s AND purge_id = %s
                    RETURNING 1
                    """
                ).format(self._table_identifier("purge_operations")),
                [status, now, self.org_id, purge_id],
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
        *,
        execution_claim: PurgeExecutionClaim,
        owned_user_playbook_ids: set[int] | None = None,
    ) -> None:
        with self.commit_scope():
            self._assert_purge_operation_execution_claim_locked(
                purge_id, execution_claim
            )
            authoritative_user_digest = self._assert_authoritative_user_identity_locked(
                purge_id, user_id
            )
        if self.purge_targets_prepared(purge_id):
            return
        if owned_user_playbook_ids is None:
            owned_user_playbook_ids = {
                int(row["user_playbook_id"])
                for row in self._fetch_all(
                    sql.SQL(
                        "SELECT user_playbook_id FROM {} WHERE user_id = %s"
                    ).format(self._table_identifier("user_playbooks")),
                    [user_id],
                )
            }
        profile_ids = [
            str(row["profile_id"])
            for row in self._fetch_all(
                sql.SQL("SELECT profile_id FROM {} WHERE user_id = %s").format(
                    self._table_identifier("profiles")
                ),
                [user_id],
            )
        ]
        purge_profile_ids, delete_profile_ids = self._partition_purge_vs_delete(
            "profile", profile_ids
        )
        purge_playbook_ids, delete_playbook_ids = self._partition_purge_vs_delete(
            "user_playbook",
            [str(value) for value in sorted(owned_user_playbook_ids)],
        )
        session_rows = self._fetch_all(
            sql.SQL("SELECT DISTINCT session_id FROM {} WHERE user_id = %s").format(
                self._table_identifier("requests")
            ),
            [user_id],
        )
        subject_ref = self._subject_ref_for_user_id(user_id)
        session_outcome_rows = self._fetch_all(
            sql.SQL(
                """SELECT count(*) AS count FROM {}
                   WHERE user_id = %s OR governance_subject_ref = %s"""
            ).format(self._table_identifier("session_outcomes")),
            [user_id, subject_ref],
        )
        counts = {
            "session_outcome": int(session_outcome_rows[0]["count"]),
            "request": self._count_where("requests", "user_id", user_id),
            "interaction": self._count_where("interactions", "user_id", user_id),
            "profile": len(delete_profile_ids),
            "user_playbook": len(delete_playbook_ids),
            "agent_success_evaluation_result": self._count_where(
                "agent_success_evaluation_result", "user_id", user_id
            ),
            "retrieved_learning_evaluation_result": self._count_where(
                "retrieved_learning_evaluation", "user_id", user_id
            ),
            "evaluation_operation_state": 3 * len(session_rows),
            "offline_tuner_reward_label": 0,
            "offline_tuner_reward_label_target_by_target_owner": 0,
            "profile_purge": len(purge_profile_ids),
            "user_playbook_purge": len(purge_playbook_ids),
        }
        for target_name, count in counts.items():
            self.record_purge_target(
                purge_id,
                target_name,
                "delete",
                "pending",
                execution_claim=execution_claim,
                target_ref="all",
                detail={"count": count},
            )
        self.record_purge_target(
            purge_id,
            "target_snapshot",
            "prepare_targets",
            "complete",
            execution_claim=execution_claim,
            target_ref="all",
            detail={
                "authoritative_user_digest": authoritative_user_digest,
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
    def hide_governance_agent_playbooks_for_rebuild(
        self,
        purge_id: str,
        *,
        execution_claim: PurgeExecutionClaim,
    ) -> list[int]:
        with self.commit_scope():
            self._assert_purge_operation_execution_claim_locked(
                purge_id, execution_claim
            )
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
                        execution_claim=execution_claim,
                        target_ref=str(agent_playbook_id),
                    )
        return ids

    @handle_exceptions
    def apply_governance_user_data_delete(
        self,
        purge_id: str,
        user_id: str,
        *,
        execution_claim: PurgeExecutionClaim,
    ) -> dict[str, int]:
        with self.commit_scope():
            self._assert_purge_operation_execution_claim_locked(
                purge_id, execution_claim
            )
            self._assert_authoritative_user_identity_locked(purge_id, user_id)
            return self._apply_governance_user_data_delete(
                purge_id,
                user_id,
                execution_claim=execution_claim,
            )

    def _apply_governance_user_data_delete(
        self,
        purge_id: str,
        user_id: str,
        *,
        execution_claim: PurgeExecutionClaim,
    ) -> dict[str, int]:
        session_ids = [
            str(row["session_id"])
            for row in self._fetch_all(
                sql.SQL("SELECT DISTINCT session_id FROM {} WHERE user_id = %s").format(
                    self._table_identifier("requests")
                ),
                [user_id],
            )
        ]
        counts = self.clear_user_data(user_id)
        counts["agent_success_evaluation_results"] = len(
            self._fetch_all(
                sql.SQL("DELETE FROM {} WHERE user_id = %s RETURNING 1").format(
                    self._table_identifier("agent_success_evaluation_result")
                ),
                [user_id],
            )
        )
        counts["retrieved_learning_evaluation_results"] = len(
            self._fetch_all(
                sql.SQL("DELETE FROM {} WHERE user_id = %s RETURNING 1").format(
                    self._table_identifier("retrieved_learning_evaluation")
                ),
                [user_id],
            )
        )
        exact_keys = [
            build_retrieved_learning_state_key(user_id, session_id)
            for session_id in session_ids
        ] + [
            build_agent_success_marker_key(self.org_id, user_id, session_id)
            for session_id in session_ids
        ]
        grade_prefixes = tuple(
            build_grade_on_demand_session_prefix(self.org_id, session_id)
            for session_id in session_ids
        )
        grade_rows = self._fetch_all(
            sql.SQL("SELECT service_name FROM {} WHERE service_name LIKE %s").format(
                self._table_identifier("_operation_state")
            ),
            [f"{GRADE_ON_DEMAND_CACHE_PREFIX}::%"],
        )
        state_keys = [
            *exact_keys,
            *[
                str(row["service_name"])
                for row in grade_rows
                if str(row["service_name"]).startswith(grade_prefixes)
            ],
        ]
        counts["evaluation_operation_states"] = (
            len(
                self._fetch_all(
                    sql.SQL(
                        "DELETE FROM {} WHERE service_name = ANY(%s) RETURNING 1"
                    ).format(self._table_identifier("_operation_state")),
                    [state_keys],
                )
            )
            if state_keys
            else 0
        )
        counts["offline_tuner_reward_labels"] = 0
        counts["offline_tuner_reward_label_targets_by_target_owner"] = 0
        target_names = {
            "session_outcomes": "session_outcome",
            "interactions": "interaction",
            "user_playbooks": "user_playbook",
            "profiles": "profile",
            "requests": "request",
            "agent_success_evaluation_results": "agent_success_evaluation_result",
            "retrieved_learning_evaluation_results": (
                "retrieved_learning_evaluation_result"
            ),
            "evaluation_operation_states": "evaluation_operation_state",
            "offline_tuner_reward_labels": "offline_tuner_reward_label",
            "offline_tuner_reward_label_targets_by_target_owner": (
                "offline_tuner_reward_label_target_by_target_owner"
            ),
            "purged_profiles": "profile_purge",
            "purged_user_playbooks": "user_playbook_purge",
        }
        for key, value in counts.items():
            self.record_purge_target(
                purge_id,
                target_names.get(key, key),
                "delete",
                "complete",
                execution_claim=execution_claim,
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
        *,
        execution_claim: PurgeExecutionClaim,
    ) -> None:
        with self.commit_scope():
            self._assert_purge_operation_execution_claim_locked(
                purge_id, execution_claim
            )
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
                execution_claim=execution_claim,
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
        self,
        purge_id: str,
        audit_event: AuditEvent,
        *,
        authoritative_user_id: str,
        execution_claim: PurgeExecutionClaim,
    ) -> PurgeOperation:
        with self.commit_scope():
            self._assert_purge_operation_execution_claim_locked(
                purge_id, execution_claim
            )
            self._assert_authoritative_user_identity_locked(
                purge_id, authoritative_user_id
            )
            audit_event.idempotency_key = audit_event.idempotency_key or purge_id
            self.append_audit_event(audit_event)
            now = _now()
            rows = self._fetch_all(
                sql.SQL(
                    """
                    UPDATE {} SET status = 'complete', error_code = NULL,
                        error_detail = NULL, updated_at = %s, completed_at = %s,
                        execution_claim_owner = NULL,
                        execution_claim_expires_at = NULL
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
        self,
        purge_id: str,
        error_code: str,
        error_detail: str,
        *,
        execution_claim: PurgeExecutionClaim,
    ) -> PurgeOperation:
        with self.commit_scope():
            self._assert_purge_operation_execution_claim_locked(
                purge_id, execution_claim
            )
            now = _now()
            rows = self._fetch_all(
                sql.SQL(
                    """
                    UPDATE {} SET status = 'failed', error_code = %s,
                        error_detail = %s, updated_at = %s, completed_at = %s,
                        execution_claim_owner = NULL,
                        execution_claim_expires_at = NULL
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
