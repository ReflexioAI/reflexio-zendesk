"""Durable learning-job queue for native PostgreSQL storage."""

from __future__ import annotations

import time
import uuid
from datetime import datetime
from typing import Any

from psycopg2 import sql

from reflexio.server.services.storage.storage_base._learning_jobs import (
    _ABSENCE_DONE_AFTER_SECONDS,
    LearningJob,
    LearningStatus,
)

from ._base import PostgresStorageBase

handle_exceptions = PostgresStorageBase.handle_exceptions


def _epoch(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    return datetime.fromisoformat(str(value)).timestamp()


def _job(row: dict[str, Any]) -> LearningJob:
    return LearningJob(
        job_id=str(row["job_id"]),
        org_id=str(row["org_id"]),
        user_id=str(row["user_id"]),
        job_type=str(row["job_type"]),
        latest_request_id=row.get("latest_request_id"),
        status=str(row["status"]),
        attempts=int(row.get("attempts") or 0),
        claim_token=row.get("claim_token"),
        covers_through=_epoch(row.get("covers_through")),
        force_extraction=bool(row.get("force_extraction")),
        skip_aggregation=bool(row.get("skip_aggregation")),
        max_attempts=int(row.get("max_attempts") or 3),
    )


class PostgresLearningJobStoreMixin:
    org_id: str
    _fetch_all: Any
    _table_identifier: Any

    @handle_exceptions
    def enqueue_learning_job(
        self,
        *,
        org_id: str,
        user_id: str,
        request_id: str,
        covers_through: float,
        job_type: str = "learning",
        force_extraction: bool = False,
        skip_aggregation: bool = False,
    ) -> str:
        rows = self._fetch_all(
            sql.SQL(
                """
                INSERT INTO {} (
                    job_id, org_id, user_id, job_type, latest_request_id,
                    covers_through, status, force_extraction, skip_aggregation
                ) VALUES (%s, %s, %s, %s, %s, to_timestamp(%s), 'pending', %s, %s)
                ON CONFLICT (org_id, user_id, job_type) WHERE status = 'pending'
                DO UPDATE SET
                    latest_request_id = EXCLUDED.latest_request_id,
                    covers_through = GREATEST({}.covers_through, EXCLUDED.covers_through),
                    force_extraction = EXCLUDED.force_extraction,
                    skip_aggregation = EXCLUDED.skip_aggregation,
                    updated_at = now()
                RETURNING job_id
                """
            ).format(
                self._table_identifier("learning_jobs"),
                self._table_identifier("learning_jobs"),
            ),
            [
                str(uuid.uuid4()),
                org_id,
                user_id,
                job_type,
                request_id,
                covers_through,
                force_extraction,
                skip_aggregation,
            ],
        )
        if not rows:
            raise RuntimeError("enqueue_learning_job RETURNING job_id returned no row")
        return str(rows[0]["job_id"])

    @handle_exceptions
    def claim_learning_jobs(
        self, *, claimed_by: str, limit: int, lease_seconds: int
    ) -> list[LearningJob]:
        if limit <= 0:
            return []
        rows = self._fetch_all(
            sql.SQL(
                """
                WITH candidates AS (
                    SELECT job_id FROM {}
                    WHERE org_id = %s AND (
                        status = 'pending'
                        OR (status = 'failed' AND attempts < max_attempts)
                        OR (status = 'claimed' AND claim_expires_at < now())
                    )
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                )
                UPDATE {} AS jobs SET
                    status = 'claimed', claimed_by = %s,
                    claim_token = gen_random_uuid()::text,
                    claim_expires_at = now() + (%s * interval '1 second'),
                    updated_at = now(), attempts = jobs.attempts + 1
                FROM candidates
                WHERE jobs.job_id = candidates.job_id
                RETURNING jobs.*
                """
            ).format(
                self._table_identifier("learning_jobs"),
                self._table_identifier("learning_jobs"),
            ),
            [self.org_id, limit, claimed_by, lease_seconds],
        )
        return [_job(row) for row in rows]

    @handle_exceptions
    def heartbeat_learning_job(
        self, *, job_id: str, claim_token: str, lease_seconds: int
    ) -> bool:
        rows = self._fetch_all(
            sql.SQL(
                """UPDATE {} SET
                       claim_expires_at = now() + (%s * interval '1 second'),
                       updated_at = now()
                   WHERE job_id = %s AND claim_token = %s AND status = 'claimed'
                   RETURNING job_id"""
            ).format(self._table_identifier("learning_jobs")),
            [lease_seconds, job_id, claim_token],
        )
        return bool(rows)

    @handle_exceptions
    def complete_learning_job(self, *, job_id: str, claim_token: str) -> int:
        rows = self._fetch_all(
            sql.SQL(
                """UPDATE {} SET status = 'done', updated_at = now()
                   WHERE job_id = %s AND claim_token = %s AND status = 'claimed'
                   RETURNING job_id"""
            ).format(self._table_identifier("learning_jobs")),
            [job_id, claim_token],
        )
        return len(rows)

    @handle_exceptions
    def fail_learning_job(
        self,
        *,
        job_id: str,
        claim_token: str,
        dead: bool,
        refund_attempt: bool = False,
    ) -> None:
        self._fetch_all(
            sql.SQL(
                """UPDATE {} SET
                       status = %s,
                       claim_token = CASE WHEN %s THEN claim_token ELSE NULL END,
                       claim_expires_at = CASE WHEN %s THEN claim_expires_at ELSE NULL END,
                       attempts = CASE WHEN %s THEN GREATEST(attempts - 1, 0) ELSE attempts END,
                       updated_at = now()
                   WHERE job_id = %s AND claim_token = %s AND status = 'claimed'
                   RETURNING job_id"""
            ).format(self._table_identifier("learning_jobs")),
            [
                "dead" if dead else "failed",
                dead,
                dead,
                refund_attempt,
                job_id,
                claim_token,
            ],
        )

    @handle_exceptions
    def list_org_ids_with_pending_learning_jobs(self) -> list[str]:
        rows = self._fetch_all(
            sql.SQL(
                """SELECT DISTINCT org_id FROM {}
                   WHERE status = 'pending'
                      OR (status = 'failed' AND attempts < max_attempts)
                      OR (status = 'claimed' AND claim_expires_at < now())
                   ORDER BY org_id"""
            ).format(self._table_identifier("learning_jobs"))
        )
        return [str(row["org_id"]) for row in rows]

    @handle_exceptions
    def get_oldest_pending_learning_job_age_seconds(self) -> float | None:
        rows = self._fetch_all(
            sql.SQL(
                """SELECT EXTRACT(EPOCH FROM (now() - MIN(created_at))) AS age_seconds
                   FROM {} WHERE status = 'pending'"""
            ).format(self._table_identifier("learning_jobs"))
        )
        value = rows[0].get("age_seconds") if rows else None
        return None if value is None else float(value)

    @handle_exceptions
    def count_learning_jobs_by_status(self, status: str) -> int:
        rows = self._fetch_all(
            sql.SQL("SELECT count(*) AS count FROM {} WHERE status = %s").format(
                self._table_identifier("learning_jobs")
            ),
            [status],
        )
        return int(rows[0]["count"]) if rows else 0

    @handle_exceptions
    def get_learning_status_for_request(
        self, *, user_id: str, request_created_at: float
    ) -> LearningStatus:
        rows = self._fetch_all(
            sql.SQL(
                "SELECT status, covers_through FROM {} WHERE org_id = %s AND user_id = %s"
            ).format(self._table_identifier("learning_jobs")),
            [self.org_id, user_id],
        )
        pending = failed = claimed = dead_covering = False
        for row in rows:
            status = str(row["status"])
            covers = (_epoch(row.get("covers_through")) or 0) >= request_created_at
            if covers and status == "done":
                return "done"
            if status == "claimed":
                claimed = True
            elif status == "pending":
                pending = True
            elif status == "failed":
                failed = True
            elif covers and status == "dead":
                dead_covering = True
        if claimed:
            return "processing"
        if pending or failed:
            return "pending"
        if dead_covering:
            return "failed"
        if time.time() - request_created_at >= _ABSENCE_DONE_AFTER_SECONDS:
            return "done"
        return "pending"
