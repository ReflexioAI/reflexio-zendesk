"""Session outcome storage for native PostgreSQL."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from psycopg2 import sql
from psycopg2.extras import Json

from reflexio.models.api_schema.domain import (
    GetSessionOutcomesRequest,
    SessionOutcomeFailureReason,
    SessionOutcomeRecord,
    SetSessionOutcomeRequest,
)
from reflexio.server.services.storage.error import SubjectWriteBarrierError
from reflexio.server.services.storage.storage_base._session_outcomes import (
    SessionOutcomeContext,
    SessionOutcomeWriteResult,
)

from ._base import PostgresStorageBase

handle_exceptions = PostgresStorageBase.handle_exceptions


def _epoch(value: Any) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp())
    return int(datetime.fromisoformat(str(value)).timestamp())


class PostgresSessionOutcomeStoreMixin:
    _fetch_all: Any
    _table_identifier: Any
    _subject_ref_for_user_id: Any
    _assert_subject_writable_locked: Any
    commit_scope: Any

    @handle_exceptions
    def get_session_outcome_context(self, session_id: str) -> SessionOutcomeContext:
        existing = self._fetch_all(
            sql.SQL("SELECT user_id, source FROM {} WHERE session_id = %s").format(
                self._table_identifier("session_outcomes")
            ),
            [session_id],
        )
        if existing:
            return SessionOutcomeContext(
                user_id=str(existing[0]["user_id"]),
                source=str(existing[0]["source"]),
                existing=True,
            )
        rows = self._fetch_all(
            sql.SQL(
                """SELECT user_id, source, created_at, request_id FROM {}
                   WHERE session_id = %s ORDER BY created_at, request_id"""
            ).format(self._table_identifier("requests")),
            [session_id],
        )
        if not rows:
            return SessionOutcomeContext()
        first = rows[0]
        return SessionOutcomeContext(
            user_id=str(first["user_id"]),
            source=str(first.get("source") or ""),
            first_request_at=_epoch(first["created_at"]),
            user_contract_violation=len({str(row["user_id"]) for row in rows}) > 1,
            source_contract_violation=len(
                {str(row.get("source") or "") for row in rows}
            )
            > 1,
        )

    @handle_exceptions
    def record_session_outcome(
        self,
        request: SetSessionOutcomeRequest,
        *,
        created_at: int,
        expected_context: SessionOutcomeContext,
    ) -> SessionOutcomeWriteResult:
        with self.commit_scope():
            existing = self._fetch_all(
                sql.SQL(
                    "SELECT user_id, source FROM {} WHERE session_id = %s FOR UPDATE"
                ).format(self._table_identifier("session_outcomes")),
                [request.session_id],
            )
            if existing:
                return SessionOutcomeWriteResult(
                    recorded=False,
                    user_id=str(existing[0]["user_id"]),
                    source=str(existing[0]["source"]),
                )
            first_rows = self._fetch_all(
                sql.SQL(
                    """SELECT user_id, source, created_at, request_id FROM {}
                       WHERE session_id = %s ORDER BY created_at, request_id
                       LIMIT 1 FOR UPDATE"""
                ).format(self._table_identifier("requests")),
                [request.session_id],
            )
            if not first_rows:
                return SessionOutcomeWriteResult(
                    recorded=False,
                    reason=SessionOutcomeFailureReason.UNKNOWN_SESSION,
                )
            first = first_rows[0]
            user_id = str(first["user_id"])
            source = str(first.get("source") or "")
            first_request_at = _epoch(first["created_at"])
            if (
                expected_context.user_id != user_id
                or expected_context.source != source
                or expected_context.first_request_at != first_request_at
            ):
                return SessionOutcomeWriteResult(
                    recorded=False,
                    user_id=user_id,
                    source=source,
                    context_changed=True,
                )
            if request.occurred_at < first_request_at:
                return SessionOutcomeWriteResult(
                    recorded=False,
                    user_id=user_id,
                    source=source,
                    reason=SessionOutcomeFailureReason.OCCURRED_BEFORE_SESSION,
                )
            subject_ref = self._subject_ref_for_user_id(user_id)
            try:
                self._assert_subject_writable_locked(subject_ref)
            except SubjectWriteBarrierError:
                return SessionOutcomeWriteResult(
                    recorded=False,
                    user_id=user_id,
                    reason=SessionOutcomeFailureReason.SUBJECT_NOT_WRITABLE,
                )
            self._fetch_all(
                sql.SQL(
                    """INSERT INTO {} (
                           user_id, session_id, outcome, occurred_at, source,
                           label, value, metadata, governance_subject_ref, created_at
                       ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       RETURNING session_id"""
                ).format(self._table_identifier("session_outcomes")),
                [
                    user_id,
                    request.session_id,
                    request.outcome.value,
                    request.occurred_at,
                    source,
                    request.label,
                    request.value,
                    Json(request.metadata) if request.metadata is not None else None,
                    subject_ref,
                    created_at,
                ],
            )
        return SessionOutcomeWriteResult(recorded=True, user_id=user_id, source=source)

    @handle_exceptions
    def get_session_outcomes(
        self, request: GetSessionOutcomesRequest
    ) -> list[SessionOutcomeRecord]:
        clauses: list[sql.Composable] = []
        params: list[Any] = []
        if request.session_ids:
            clauses.append(sql.SQL("session_id = ANY(%s)"))
            params.append(request.session_ids)
        for column, value in (
            ("user_id", request.user_id),
            ("source", request.source),
            ("outcome", request.outcome.value if request.outcome else None),
            ("label", request.label),
        ):
            if value is not None:
                clauses.append(sql.SQL("{} = %s").format(sql.Identifier(column)))
                params.append(value)
        if request.start_time is not None:
            clauses.append(sql.SQL("occurred_at >= %s"))
            params.append(request.start_time)
        if request.end_time is not None:
            clauses.append(sql.SQL("occurred_at <= %s"))
            params.append(request.end_time)
        where = (
            sql.SQL(" WHERE ") + sql.SQL(" AND ").join(clauses)
            if clauses
            else sql.SQL("")
        )
        rows = self._fetch_all(
            sql.SQL(
                """SELECT user_id, session_id, outcome, occurred_at, source,
                          label, value, metadata, created_at FROM {}{}
                   ORDER BY occurred_at DESC, user_id, session_id LIMIT %s OFFSET %s"""
            ).format(self._table_identifier("session_outcomes"), where),
            [*params, request.top_k, request.offset],
        )
        return [
            SessionOutcomeRecord(
                user_id=str(row["user_id"]),
                session_id=str(row["session_id"]),
                outcome=row["outcome"],
                occurred_at=int(row["occurred_at"]),
                source=str(row["source"]),
                label=row.get("label"),
                value=row.get("value"),
                metadata=row.get("metadata"),
                created_at=int(row["created_at"]),
            )
            for row in rows
        ]

    @handle_exceptions
    def clear_session_outcomes_for_user(self, user_id: str) -> dict[str, int]:
        subject_ref = self._subject_ref_for_user_id(user_id)
        rows = self._fetch_all(
            sql.SQL(
                """DELETE FROM {} WHERE user_id = %s OR governance_subject_ref = %s
                   RETURNING 1"""
            ).format(self._table_identifier("session_outcomes")),
            [user_id, subject_ref],
        )
        return {"session_outcomes": len(rows)}
