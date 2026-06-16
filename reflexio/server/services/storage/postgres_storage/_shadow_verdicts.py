"""Postgres CRUD for ``shadow_comparison_verdicts``."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from psycopg2 import sql

from reflexio.models.api_schema.eval_overview_schema import (
    ShadowComparisonOutput,
    ShadowComparisonVerdict,
)
from reflexio.server.services.storage.postgres_storage._base import PostgresStorageBase

handle_exceptions = PostgresStorageBase.handle_exceptions
_MAX_SAFE_EPOCH_TS = 253_402_300_799


def _epoch_to_dt(ts: int) -> datetime:
    return datetime.fromtimestamp(max(0, min(ts, _MAX_SAFE_EPOCH_TS)), tz=UTC)


def _parse_dt(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return raw if raw.tzinfo is not None else raw.replace(tzinfo=UTC)
    value = str(raw)
    if value.endswith("Z") or "+" in value or "-" in value[10:]:
        return datetime.fromisoformat(value)
    return datetime.fromisoformat(value.replace(" ", "T")).replace(tzinfo=UTC)


def _row_to_verdict(row: dict[str, Any]) -> ShadowComparisonVerdict:
    return ShadowComparisonVerdict(
        verdict_id=row["verdict_id"],
        interaction_id=row["interaction_id"],
        session_id=row["session_id"],
        agent_version=row["agent_version"],
        reflexio_is_request_1=bool(row["reflexio_is_request_1"]),
        output=ShadowComparisonOutput(
            better_request=row["better_request"],
            is_significantly_better=bool(row["is_significantly_better"]),
            comparison_reason=row.get("comparison_reason"),
        ),
        judge_prompt_version=row["judge_prompt_version"],
        created_at=_parse_dt(row["created_at"]),
    )


class PostgresShadowVerdictsMixin:
    """Postgres implementation of the shadow_comparison_verdicts contract."""

    _fetch_all: Callable[..., list[dict[str, Any]]]
    _table_identifier: Callable[[str], sql.Composable]

    @handle_exceptions
    def save_shadow_comparison_verdict(
        self, verdict: ShadowComparisonVerdict
    ) -> ShadowComparisonVerdict:
        rows = self._fetch_all(
            sql.SQL(
                """
                INSERT INTO {} (
                    interaction_id, session_id, agent_version,
                    reflexio_is_request_1, better_request,
                    is_significantly_better, comparison_reason,
                    judge_prompt_version, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """
            ).format(self._table_identifier("shadow_comparison_verdicts")),
            [
                verdict.interaction_id,
                verdict.session_id,
                verdict.agent_version,
                verdict.reflexio_is_request_1,
                verdict.output.better_request,
                verdict.output.is_significantly_better,
                verdict.output.comparison_reason,
                verdict.judge_prompt_version,
                verdict.created_at,
            ],
        )
        return _row_to_verdict(rows[0])

    @handle_exceptions
    def get_shadow_comparison_verdict(
        self, verdict_id: int
    ) -> ShadowComparisonVerdict | None:
        rows = self._fetch_all(
            sql.SQL("SELECT * FROM {} WHERE verdict_id = %s").format(
                self._table_identifier("shadow_comparison_verdicts")
            ),
            [verdict_id],
        )
        return _row_to_verdict(rows[0]) if rows else None

    @handle_exceptions
    def get_shadow_comparison_verdicts(
        self,
        from_ts: int,
        to_ts: int,
        judge_prompt_version: str,
    ) -> list[ShadowComparisonVerdict]:
        rows = self._fetch_all(
            sql.SQL(
                """
                SELECT *
                FROM {}
                WHERE created_at >= %s
                  AND created_at <= %s
                  AND judge_prompt_version = %s
                ORDER BY created_at ASC
                """
            ).format(self._table_identifier("shadow_comparison_verdicts")),
            [_epoch_to_dt(from_ts), _epoch_to_dt(to_ts), judge_prompt_version],
        )
        return [_row_to_verdict(row) for row in rows]

    @handle_exceptions
    def delete_shadow_comparison_verdicts_by_session(self, session_id: str) -> int:
        rows = self._fetch_all(
            sql.SQL("DELETE FROM {} WHERE session_id = %s RETURNING 1").format(
                self._table_identifier("shadow_comparison_verdicts")
            ),
            [session_id],
        )
        return len(rows)
