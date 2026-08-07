"""Retrieved-learning evaluation state and result storage for PostgreSQL."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from psycopg2 import sql
from psycopg2.extras import Json

from reflexio.models.api_schema.service_schemas import (
    AgentSuccessEvaluationResult,
    RetrievedLearningEvaluationResult,
)
from reflexio.server.services.storage.storage_base.retrieved_learning_state import (
    CANONICAL_RETRIEVED_KINDS,
    DEFAULT_TRANSCRIPT_CHAR_LIMIT,
    RETRIEVED_LEARNING_EVALUATION_VERSION,
    TERMINAL_RETRIEVED_STATUSES,
    BoundedRetrievedLearningSnapshot,
    RetrievedLearningCommitResult,
    SessionFingerprintBuilder,
    append_bounded_snapshot_interaction,
    build_retrieved_learning_state_key,
)

from ._base import PostgresStorageBase

handle_exceptions = PostgresStorageBase.handle_exceptions


def _epoch(value: Any) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp())
    return int(datetime.fromisoformat(str(value)).timestamp())


def _refs(raw: Any) -> list[tuple[str, str]]:
    if not isinstance(raw, list):
        return []
    return [
        (str(item.get("kind") or ""), str(item.get("learning_id") or ""))
        for item in raw
        if isinstance(item, dict)
        and str(item.get("kind") or "") in CANONICAL_RETRIEVED_KINDS
        and item.get("learning_id")
    ]


def _result(row: dict[str, Any]) -> RetrievedLearningEvaluationResult:
    return RetrievedLearningEvaluationResult(
        result_id=int(row["result_id"]),
        user_id=str(row["user_id"]),
        session_id=str(row["session_id"]),
        agent_version=str(row.get("agent_version") or ""),
        interaction_id=(
            int(row["interaction_id"])
            if row.get("interaction_id") is not None
            else None
        ),
        interaction_created_at=(
            int(row["interaction_created_at"])
            if row.get("interaction_created_at") is not None
            else None
        ),
        kind=row["kind"],
        learning_id=str(row["learning_id"]),
        is_relevant=row.get("is_relevant"),
        relevance_reason=str(row.get("relevance_reason") or ""),
        impact=row.get("impact"),
        impact_reason=str(row.get("impact_reason") or ""),
        created_at=int(row["created_at"]),
    )


class PostgresRetrievedLearningMixin:
    org_id: str
    _fetch_all: Any
    _table_identifier: Any
    _subject_ref_for_user_id: Any
    _assert_subject_writable_locked: Any
    commit_scope: Any

    def _state(self, key: str, *, for_update: bool = False) -> dict[str, Any]:
        rows = self._fetch_all(
            sql.SQL("SELECT operation_state FROM {} WHERE service_name = %s{}").format(
                self._table_identifier("_operation_state"),
                sql.SQL(" FOR UPDATE") if for_update else sql.SQL(""),
            ),
            [key],
        )
        return dict(rows[0].get("operation_state") or {}) if rows else {}

    def _put_state(self, key: str, state: dict[str, Any]) -> None:
        self._fetch_all(
            sql.SQL(
                """INSERT INTO {} (service_name, operation_state, updated_at)
                   VALUES (%s, %s, now())
                   ON CONFLICT (service_name) DO UPDATE SET
                       operation_state = EXCLUDED.operation_state,
                       updated_at = EXCLUDED.updated_at RETURNING service_name"""
            ).format(self._table_identifier("_operation_state")),
            [key, Json(state)],
        )

    def _fingerprint(self, user_id: str, session_id: str) -> str:
        rows = self._fetch_all(
            sql.SQL(
                """SELECT i.interaction_id, i.role,
                          left(i.content, %s) AS content, i.retrieved_learnings
                   FROM {} i JOIN {} r ON i.request_id = r.request_id
                   WHERE r.session_id = %s AND i.user_id = %s
                   ORDER BY i.created_at, i.interaction_id"""
            ).format(
                self._table_identifier("interactions"),
                self._table_identifier("requests"),
            ),
            [DEFAULT_TRANSCRIPT_CHAR_LIMIT, session_id, user_id],
        )
        builder = SessionFingerprintBuilder()
        for row in rows:
            builder.add(
                int(row["interaction_id"]),
                _refs(row.get("retrieved_learnings")),
                str(row.get("role") or "User"),
                str(row.get("content") or ""),
            )
        return builder.hexdigest()

    @handle_exceptions
    def update_agent_success_evaluation_result_tags(
        self,
        result_id: int,
        tags: list[str],
        *,
        expected_result: AgentSuccessEvaluationResult,
    ) -> bool:
        with self.commit_scope():
            rows = self._fetch_all(
                sql.SQL(
                    "SELECT user_id, governance_subject_ref FROM {} WHERE result_id = %s FOR UPDATE"
                ).format(self._table_identifier("agent_success_evaluation_result")),
                [result_id],
            )
            if not rows:
                return False
            subject_ref = rows[0].get("governance_subject_ref") or (
                self._subject_ref_for_user_id(str(rows[0]["user_id"]))
            )
            self._assert_subject_writable_locked(subject_ref)
            updated = self._fetch_all(
                sql.SQL(
                    """UPDATE {} SET tags = %s WHERE result_id = %s AND tags IS NULL
                       AND agent_version = %s AND is_success = %s
                       AND failure_type IS NOT DISTINCT FROM %s
                       AND failure_reason IS NOT DISTINCT FROM %s
                       AND regular_vs_shadow IS NOT DISTINCT FROM %s
                       AND number_of_correction_per_session = %s
                       AND user_turns_to_resolution IS NOT DISTINCT FROM %s
                       AND is_escalated = %s RETURNING result_id"""
                ).format(self._table_identifier("agent_success_evaluation_result")),
                [
                    Json(tags),
                    result_id,
                    expected_result.agent_version,
                    expected_result.is_success,
                    expected_result.failure_type,
                    expected_result.failure_reason,
                    expected_result.regular_vs_shadow.value
                    if expected_result.regular_vs_shadow
                    else None,
                    expected_result.number_of_correction_per_session,
                    expected_result.user_turns_to_resolution,
                    expected_result.is_escalated,
                ],
            )
        return bool(updated)

    @handle_exceptions
    def begin_retrieved_learning_evaluation_run(
        self, user_id: str, session_id: str
    ) -> int:
        key = build_retrieved_learning_state_key(user_id, session_id)
        with self.commit_scope():
            self._assert_subject_writable_locked(self._subject_ref_for_user_id(user_id))
            state = self._state(key, for_update=True)
            generation = int(state.get("generation") or 0) + 1
            state.update(
                {
                    "user_id": user_id,
                    "session_id": session_id,
                    "generation": generation,
                    "evaluation_version": RETRIEVED_LEARNING_EVALUATION_VERSION,
                    "attempted_at": int(datetime.now().timestamp()),
                }
            )
            state.setdefault("status", "pending")
            state.setdefault("session_fingerprint", "")
            self._put_state(key, state)
        return generation

    @handle_exceptions
    def load_bounded_retrieved_learning_snapshot(
        self,
        user_id: str,
        session_id: str,
        raw_ref_limit: int = 5_000,
        transcript_char_limit: int = DEFAULT_TRANSCRIPT_CHAR_LIMIT,
    ) -> BoundedRetrievedLearningSnapshot:
        snapshot = BoundedRetrievedLearningSnapshot()
        request_rows = self._fetch_all(
            sql.SQL(
                """SELECT min(created_at) AS earliest,
                          (array_agg(agent_version ORDER BY created_at DESC))[1] AS agent_version
                   FROM {} WHERE session_id = %s AND user_id = %s"""
            ).format(self._table_identifier("requests")),
            [session_id, user_id],
        )
        if request_rows and request_rows[0].get("earliest"):
            snapshot.earliest_request_created_at = _epoch(request_rows[0]["earliest"])
            snapshot.agent_version = str(request_rows[0].get("agent_version") or "")
        rows = self._fetch_all(
            sql.SQL(
                """SELECT i.interaction_id, i.role,
                          left(i.content, %s) AS content,
                          left(i.content, %s) AS fp_content,
                          i.created_at, i.retrieved_learnings
                   FROM {} i JOIN {} r ON i.request_id = r.request_id
                   WHERE r.session_id = %s AND i.user_id = %s
                   ORDER BY i.created_at, i.interaction_id"""
            ).format(
                self._table_identifier("interactions"),
                self._table_identifier("requests"),
            ),
            [
                transcript_char_limit,
                DEFAULT_TRANSCRIPT_CHAR_LIMIT,
                session_id,
                user_id,
            ],
        )
        builder = SessionFingerprintBuilder()
        remaining = transcript_char_limit
        for row in rows:
            refs = _refs(row.get("retrieved_learnings"))
            snapshot.raw_attachment_count += len(refs)
            builder.add(
                int(row["interaction_id"]),
                refs,
                str(row.get("role") or "User"),
                str(row.get("fp_content") or ""),
            )
            if snapshot.raw_attachment_count > raw_ref_limit:
                snapshot.attachment_limit_exceeded = True
                snapshot.interactions.clear()
                remaining = 0
                continue
            remaining = append_bounded_snapshot_interaction(
                snapshot,
                interaction_id=int(row["interaction_id"]),
                role=str(row.get("role") or "User"),
                content=str(row.get("content") or ""),
                created_at=_epoch(row["created_at"]),
                refs=refs,
                transcript_chars_remaining=remaining,
            )
        snapshot.precomputed_fingerprint = builder.hexdigest()
        return snapshot

    @handle_exceptions
    def get_matching_retrieved_learning_terminal_state(
        self, user_id: str, session_id: str, session_fingerprint: str
    ) -> dict[str, Any] | None:
        key = build_retrieved_learning_state_key(user_id, session_id)
        with self.commit_scope():
            state = self._state(key, for_update=True)
            live = self._fingerprint(user_id, session_id)
        matched = (
            state.get("status") in TERMINAL_RETRIEVED_STATUSES
            and state.get("evaluation_version") == RETRIEVED_LEARNING_EVALUATION_VERSION
            and state.get("session_fingerprint") == session_fingerprint
            and live == session_fingerprint
        )
        return state if matched else None

    def _attached(self, user_id: str, session_id: str) -> set[tuple[int, str, str]]:
        rows = self._fetch_all(
            sql.SQL(
                """SELECT i.interaction_id, i.retrieved_learnings FROM {} i
                   JOIN {} r ON i.request_id = r.request_id
                   WHERE r.session_id = %s AND i.user_id = %s"""
            ).format(
                self._table_identifier("interactions"),
                self._table_identifier("requests"),
            ),
            [session_id, user_id],
        )
        return {
            (int(row["interaction_id"]), kind, learning_id)
            for row in rows
            for kind, learning_id in _refs(row.get("retrieved_learnings"))
        }

    def _resolvable(
        self, user_id: str, results: list[RetrievedLearningEvaluationResult]
    ) -> set[tuple[str, str]]:
        by_kind: dict[str, list[str]] = {}
        for result in results:
            by_kind.setdefault(result.kind, []).append(result.learning_id)
        resolved: set[tuple[str, str]] = set()
        specs = (
            ("profile", "profiles", "profile_id", True),
            ("user_playbook", "user_playbooks", "user_playbook_id", True),
            ("agent_playbook", "agent_playbooks", "agent_playbook_id", False),
        )
        for kind, table, pk, scoped in specs:
            values = by_kind.get(kind, [])
            if not values:
                continue
            params: list[Any] = [values]
            query = sql.SQL("SELECT {} FROM {} WHERE {}::text = ANY(%s)").format(
                sql.Identifier(pk),
                self._table_identifier(table),
                sql.Identifier(pk),
            )
            if scoped:
                query += sql.SQL(" AND user_id = %s")
                params.append(user_id)
            rows = self._fetch_all(query, params)
            resolved.update((kind, str(row[pk])) for row in rows)
        return resolved

    @handle_exceptions
    def replace_retrieved_learning_evaluation_results(
        self,
        user_id: str,
        session_id: str,
        generation: int,
        session_fingerprint: str,
        proposed_status: str,
        diagnostics: dict[str, Any],
        results: list[RetrievedLearningEvaluationResult],
    ) -> RetrievedLearningCommitResult:
        if proposed_status not in ("complete", "degraded"):
            raise ValueError(f"invalid proposed_status: {proposed_status}")
        key = build_retrieved_learning_state_key(user_id, session_id)
        subject_ref = self._subject_ref_for_user_id(user_id)
        with self.commit_scope():
            self._assert_subject_writable_locked(subject_ref)
            state = self._state(key, for_update=True)
            if int(state.get("generation") or 0) != generation:
                return RetrievedLearningCommitResult(disposition="superseded")
            if self._fingerprint(user_id, session_id) != session_fingerprint:
                return RetrievedLearningCommitResult(disposition="stale")
            attached = self._attached(user_id, session_id)
            resolvable = self._resolvable(user_id, results)
            kept = [
                result
                for result in results
                if result.interaction_id is not None
                and (result.interaction_id, result.kind, result.learning_id) in attached
                and (result.kind, result.learning_id) in resolvable
            ]
            final_status = proposed_status if kept else "not_applicable"
            self._fetch_all(
                sql.SQL("DELETE FROM {} WHERE user_id = %s AND session_id = %s").format(
                    self._table_identifier("retrieved_learning_evaluation")
                ),
                [user_id, session_id],
            )
            for result in kept:
                self._fetch_all(
                    sql.SQL(
                        """INSERT INTO {} (
                               user_id, session_id, agent_version, interaction_id,
                               interaction_created_at, kind, learning_id, is_relevant,
                               relevance_reason, impact, impact_reason, created_at,
                               governance_subject_ref
                           ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                     %s, %s, %s) RETURNING result_id"""
                    ).format(self._table_identifier("retrieved_learning_evaluation")),
                    [
                        user_id,
                        session_id,
                        result.agent_version,
                        result.interaction_id,
                        result.interaction_created_at,
                        result.kind,
                        result.learning_id,
                        result.is_relevant,
                        result.relevance_reason,
                        result.impact,
                        result.impact_reason,
                        result.created_at,
                        subject_ref,
                    ],
                )
            state.update(diagnostics)
            state.update(
                {
                    "user_id": user_id,
                    "session_id": session_id,
                    "generation": generation,
                    "evaluation_version": RETRIEVED_LEARNING_EVALUATION_VERSION,
                    "status": final_status,
                    "session_fingerprint": session_fingerprint,
                    "resolvable_count": len(resolvable),
                    "committed_count": len(kept),
                    "completed_at": int(datetime.now().timestamp()),
                }
            )
            self._put_state(key, state)
        return RetrievedLearningCommitResult(
            disposition="applied", status=final_status, committed_count=len(kept)
        )

    @handle_exceptions
    def finish_retrieved_learning_evaluation_run(
        self,
        user_id: str,
        session_id: str,
        generation: int,
        status: str,
        diagnostics: dict[str, Any],
    ) -> None:
        if status not in ("failed", "pending"):
            raise ValueError(f"invalid finish status: {status}")
        key = build_retrieved_learning_state_key(user_id, session_id)
        with self.commit_scope():
            state = self._state(key, for_update=True)
            if int(state.get("generation") or 0) != generation:
                return
            state.update(diagnostics)
            state["status"] = status
            self._put_state(key, state)

    @handle_exceptions
    def get_retrieved_learning_evaluation_results(
        self,
        user_id: str | None = None,
        session_id: str | None = None,
        from_ts: int | None = None,
        to_ts: int | None = None,
        limit: int = 100,
    ) -> list[RetrievedLearningEvaluationResult]:
        clauses: list[sql.Composable] = []
        params: list[Any] = []
        for column, value in (("user_id", user_id), ("session_id", session_id)):
            if value is not None:
                clauses.append(sql.SQL("{} = %s").format(sql.Identifier(column)))
                params.append(value)
        if from_ts is not None:
            clauses.append(sql.SQL("interaction_created_at >= %s"))
            params.append(from_ts)
        if to_ts is not None:
            clauses.append(sql.SQL("interaction_created_at <= %s"))
            params.append(to_ts)
        where = (
            sql.SQL(" WHERE ") + sql.SQL(" AND ").join(clauses)
            if clauses
            else sql.SQL("")
        )
        order = (
            sql.SQL("interaction_created_at DESC, interaction_id DESC, result_id DESC")
            if from_ts is not None or to_ts is not None
            else sql.SQL("created_at DESC, result_id DESC")
        )
        rows = self._fetch_all(
            sql.SQL("SELECT * FROM {}{} ORDER BY {} LIMIT %s").format(
                self._table_identifier("retrieved_learning_evaluation"), where, order
            ),
            [*params, limit],
        )
        return [_result(row) for row in rows]

    @handle_exceptions
    def get_retrieved_learning_evaluation_results_in_window(
        self, from_ts: int, to_ts: int, *, agent_version: str | None = None
    ) -> list[RetrievedLearningEvaluationResult]:
        query = sql.SQL(
            "SELECT * FROM {} WHERE created_at >= %s AND created_at <= %s"
        ).format(self._table_identifier("retrieved_learning_evaluation"))
        params: list[Any] = [from_ts, to_ts]
        if agent_version is not None:
            query += sql.SQL(" AND agent_version = %s")
            params.append(agent_version)
        query += sql.SQL(" ORDER BY created_at, result_id")
        return [_result(row) for row in self._fetch_all(query, params)]
