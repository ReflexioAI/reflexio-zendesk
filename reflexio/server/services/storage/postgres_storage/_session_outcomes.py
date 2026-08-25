"""Session outcome storage for native PostgreSQL."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import uuid4

from psycopg2 import sql
from psycopg2.extras import Json

from reflexio.models.api_schema.domain import (
    GetSessionOutcomesRequest,
    SessionOutcomeFailureReason,
    SessionOutcomeRecord,
    SetSessionOutcomeRequest,
)
from reflexio.server.services.storage.error import SubjectWriteBarrierError
from reflexio.server.services.storage.session_outcome_identity import (
    OUTCOME_ALLOWED_VALUES,
    OUTCOME_FINALIZATION_RULE,
    OUTCOME_SCHEMA_VERSION,
    CanonicalTrajectoryDigestAccumulator,
    CanonicalTrajectoryDigestResult,
    outcome_contract_digest,
)
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


def _canonical_metadata_json(metadata: object) -> str | None:
    if metadata is None:
        return None
    return json.dumps(
        metadata,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _metadata_matches(*, stored_metadata: object, request_metadata: object) -> bool:
    try:
        return _canonical_metadata_json(stored_metadata) == _canonical_metadata_json(
            request_metadata
        )
    except (RecursionError, TypeError, ValueError):
        return False


class PostgresSessionOutcomeStoreMixin:
    _fetch_all: Any
    _table_identifier: Any
    _subject_ref_for_user_id: Any
    _assert_subject_writable_locked: Any
    commit_scope: Any

    def _canonical_session_trajectory_snapshot(
        self, session_id: str
    ) -> CanonicalTrajectoryDigestResult:
        request_rows = self._fetch_all(
            sql.SQL(
                """
                SELECT request_id, user_id, created_at, source, agent_version,
                       session_id, evaluation_only, retrieval_experiment_id,
                       retrieval_experiment_arm, governance_subject_ref
                FROM {}
                WHERE session_id = %s
                ORDER BY created_at ASC, request_id ASC
                """
            ).format(self._table_identifier("requests")),
            [session_id],
        )
        accumulator = CanonicalTrajectoryDigestAccumulator(session_id)
        first_request = dict(request_rows[0]) if request_rows else None
        for request_row in request_rows:
            accumulator.start_request(request_row)
            interactions = self._fetch_all(
                sql.SQL(
                    """
                    SELECT interaction_id, user_id, request_id, created_at,
                           content, role, token_count, user_action,
                           user_action_description, interacted_image_url,
                           ''::text AS image_encoding, shadow_content,
                           expert_content, tools_used, citations,
                           retrieved_learnings
                    FROM {}
                    WHERE request_id = %s
                    ORDER BY created_at ASC, interaction_id ASC
                    """
                ).format(self._table_identifier("interactions")),
                [request_row["request_id"]],
            )
            for interaction in interactions:
                accumulator.add_interaction(interaction)
            accumulator.finish_request()
        return CanonicalTrajectoryDigestResult(
            digest=accumulator.hexdigest(),
            first_request=first_request,
            request_count=len(request_rows),
        )

    @staticmethod
    def _outcome_contract_digest(source: str) -> str:
        return outcome_contract_digest(
            source=source,
            schema_version=OUTCOME_SCHEMA_VERSION,
            allowed_values=OUTCOME_ALLOWED_VALUES,
            finalization_rule=OUTCOME_FINALIZATION_RULE,
        )

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
                """
                SELECT user_id, source, created_at, request_id
                FROM {}
                WHERE session_id = %s
                ORDER BY created_at ASC, request_id ASC
                """
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
            existing_rows = self._fetch_all(
                sql.SQL("SELECT * FROM {} WHERE session_id = %s FOR UPDATE").format(
                    self._table_identifier("session_outcomes")
                ),
                [request.session_id],
            )
            if existing_rows:
                existing = existing_rows[0]
                snapshot = self._canonical_session_trajectory_snapshot(
                    request.session_id
                )
                first = snapshot.first_request
                source = (
                    str(first.get("source") or "")
                    if first is not None
                    else str(existing["source"])
                )
                subject_ref = (
                    str(
                        first.get("governance_subject_ref")
                        or self._subject_ref_for_user_id(str(first["user_id"]))
                    )
                    if first is not None
                    else str(existing["governance_subject_ref"])
                )
                contract_digest = self._outcome_contract_digest(source)
                stored_contract_digest = existing.get("outcome_contract_digest")
                stored_trajectory_digest = existing.get("finalized_trajectory_digest")
                server_context_matches = first is None or (
                    str(existing["user_id"]) == str(first["user_id"])
                    and str(existing["source"]) == source
                    and str(existing["governance_subject_ref"]) == subject_ref
                )
                exact_retry = (
                    existing["outcome"] == str(request.outcome)
                    and int(existing["occurred_at"]) == request.occurred_at
                    and existing.get("label") == request.label
                    and existing.get("value") == request.value
                    and _metadata_matches(
                        stored_metadata=existing.get("metadata"),
                        request_metadata=request.metadata,
                    )
                    and server_context_matches
                    and (
                        stored_contract_digest is None
                        or stored_contract_digest == contract_digest
                    )
                    and (
                        stored_trajectory_digest is None
                        or stored_trajectory_digest == snapshot.digest
                    )
                )
                identity_is_complete = all(
                    existing.get(column) is not None
                    for column in (
                        "outcome_id",
                        "outcome_revision",
                        "outcome_contract_digest",
                        "finalized_trajectory_digest",
                    )
                )
                return SessionOutcomeWriteResult(
                    recorded=False,
                    user_id=str(existing["user_id"]),
                    source=str(existing["source"]),
                    reason=(
                        None
                        if exact_retry
                        else SessionOutcomeFailureReason.CONFLICTING_FINALIZATION
                    ),
                    outcome_id=(
                        str(existing["outcome_id"]) if identity_is_complete else None
                    ),
                    outcome_revision=(
                        int(existing["outcome_revision"])
                        if identity_is_complete
                        else None
                    ),
                    outcome_contract_digest=(
                        str(stored_contract_digest) if identity_is_complete else None
                    ),
                    finalized_trajectory_digest=(
                        str(stored_trajectory_digest) if identity_is_complete else None
                    ),
                )

            snapshot = self._canonical_session_trajectory_snapshot(request.session_id)
            if snapshot.request_count == 0 or snapshot.first_request is None:
                return SessionOutcomeWriteResult(
                    recorded=False,
                    reason=SessionOutcomeFailureReason.UNKNOWN_SESSION,
                )
            first = snapshot.first_request
            user_id = str(first["user_id"])
            source = str(first.get("source") or "")
            first_request_at = _epoch(first["created_at"])
            subject_ref = str(
                first.get("governance_subject_ref")
                or self._subject_ref_for_user_id(user_id)
            )
            try:
                self._assert_subject_writable_locked(subject_ref)
            except SubjectWriteBarrierError:
                return SessionOutcomeWriteResult(
                    recorded=False,
                    user_id=user_id,
                    reason=SessionOutcomeFailureReason.SUBJECT_NOT_WRITABLE,
                )
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
            contract_digest = self._outcome_contract_digest(source)
            outcome_id = uuid4().hex
            self._fetch_all(
                sql.SQL(
                    """
                    INSERT INTO {} (
                        outcome_id, outcome_revision, user_id, session_id,
                        outcome, occurred_at, source, label, value, metadata,
                        outcome_contract_digest, finalized_trajectory_digest,
                        governance_subject_ref, created_at
                    ) VALUES (
                        %s, 1, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    RETURNING session_id
                    """
                ).format(self._table_identifier("session_outcomes")),
                [
                    outcome_id,
                    user_id,
                    request.session_id,
                    str(request.outcome),
                    request.occurred_at,
                    source,
                    request.label,
                    request.value,
                    Json(request.metadata) if request.metadata is not None else None,
                    contract_digest,
                    snapshot.digest,
                    subject_ref,
                    created_at,
                ],
            )
        return SessionOutcomeWriteResult(
            recorded=True,
            user_id=user_id,
            source=source,
            outcome_id=outcome_id,
            outcome_revision=1,
            outcome_contract_digest=contract_digest,
            finalized_trajectory_digest=snapshot.digest,
        )

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
            ("outcome", str(request.outcome) if request.outcome else None),
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
                """
                SELECT outcome_id, outcome_revision, user_id, session_id,
                       outcome, occurred_at, source, label, value, metadata,
                       outcome_contract_digest, finalized_trajectory_digest,
                       created_at
                FROM {}{}
                ORDER BY occurred_at DESC, user_id, session_id
                LIMIT %s OFFSET %s
                """
            ).format(self._table_identifier("session_outcomes"), where),
            [*params, request.top_k, request.offset],
        )
        return [
            SessionOutcomeRecord(
                outcome_id=row.get("outcome_id"),
                outcome_revision=row.get("outcome_revision"),
                user_id=str(row["user_id"]),
                session_id=str(row["session_id"]),
                outcome=row["outcome"],
                occurred_at=int(row["occurred_at"]),
                source=str(row["source"]),
                label=row.get("label"),
                value=row.get("value"),
                metadata=row.get("metadata"),
                outcome_contract_digest=row.get("outcome_contract_digest"),
                finalized_trajectory_digest=row.get("finalized_trajectory_digest"),
                created_at=int(row["created_at"]),
            )
            for row in rows
        ]

    @handle_exceptions
    def clear_session_outcomes_for_user(self, user_id: str) -> dict[str, int]:
        rows = self._fetch_all(
            sql.SQL("DELETE FROM {} WHERE user_id = %s RETURNING 1").format(
                self._table_identifier("session_outcomes")
            ),
            [user_id],
        )
        return {"session_outcomes": len(rows)}
