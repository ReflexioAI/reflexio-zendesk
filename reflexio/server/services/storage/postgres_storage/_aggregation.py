"""Durable incremental playbook-aggregation state for native PostgreSQL."""

from __future__ import annotations

import json
from typing import Any

from psycopg2 import sql
from psycopg2.extras import Json, RealDictCursor

from reflexio.server.services.storage.storage_base.playbook import (
    AGGREGATION_INVALIDATION_RETENTION_SECONDS,
    AGGREGATION_RETRY_BASE_SECONDS,
    AGGREGATION_RETRY_MAX_SECONDS,
    AggregationDisposition,
    PlaybookAggregationBacklog,
    PlaybookAggregationClaim,
    PlaybookAggregationClusterMatch,
    PlaybookAggregationInvalidation,
    PlaybookAggregationRebuildSample,
    PlaybookAggregationRerunSnapshot,
)

from ._base import _USER_PLAYBOOK_COLUMNS_WITH_EMBEDDING


class PostgresPlaybookAggregationStoreMixin:
    supports_incremental_playbook_aggregation = True

    _writer_connection: Any
    _table_identifier: Any
    _row_to_user_playbook: Any

    def _aggregation_table(self, name: str) -> Any:
        return self._table_identifier(name)

    def _schedule_playbook_aggregation_with_cursor(
        self, cur: Any, agent_version: str
    ) -> None:
        """Arm one version on the caller's transaction-bound cursor."""
        cur.execute(
            sql.SQL(
                "INSERT INTO {} (agent_version, pending, next_attempt_at) "
                "VALUES (%s, true, floor(extract(epoch FROM clock_timestamp()))::bigint) "
                "ON CONFLICT (agent_version) DO UPDATE SET pending=true"
            ).format(self._aggregation_table("playbook_aggregation_state")),
            [agent_version],
        )

    def schedule_playbook_aggregation(self, agent_version: str) -> None:
        if not agent_version.strip():
            raise ValueError("agent_version must be non-empty")
        with self._writer_connection(operation="aggregation.schedule") as conn:
            with conn.cursor() as cur:
                self._schedule_playbook_aggregation_with_cursor(cur, agent_version)
            conn.commit()

    def repair_playbook_aggregation_pending_state(
        self, *, limit: int = 100
    ) -> list[str]:
        if limit <= 0:
            return []
        with self._writer_connection(operation="aggregation.repair_pending") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "DELETE FROM {} WHERE processed_at IS NOT NULL AND "
                        "processed_at < floor(extract(epoch FROM "
                        "clock_timestamp()))::bigint-%s"
                    ).format(
                        self._aggregation_table("playbook_aggregation_invalidation")
                    ),
                    [AGGREGATION_INVALIDATION_RETENTION_SECONDS],
                )
                cur.execute(
                    sql.SQL(
                        "WITH work(agent_version) AS (SELECT p.agent_version FROM {} p "
                        "LEFT JOIN {} ps ON ps.agent_version=p.agent_version "
                        "WHERE p.status IS NULL AND NULLIF(btrim(p.content), '') IS NOT NULL "
                        "AND NULLIF(btrim(p.agent_version), '') IS NOT NULL AND "
                        "p.user_playbook_id>=COALESCE(ps.intake_floor_user_playbook_id, 0) "
                        "AND NOT EXISTS ("
                        "SELECT 1 FROM {} i WHERE i.agent_version=p.agent_version "
                        "AND i.user_playbook_id=p.user_playbook_id) UNION "
                        "SELECT agent_version FROM {} WHERE disposition='residual' UNION "
                        "SELECT agent_version FROM {} WHERE processed_at IS NULL UNION "
                        "SELECT agent_version FROM {} WHERE dirty=true OR state='rebuilding'), "
                        "versions AS (SELECT DISTINCT w.agent_version FROM work w LEFT JOIN {} s "
                        "ON s.agent_version=w.agent_version WHERE COALESCE(s.pending, false)=false "
                        "ORDER BY w.agent_version LIMIT %s), inserted AS (INSERT INTO {} "
                        "(agent_version, pending, next_attempt_at) SELECT agent_version, "
                        "true, floor(extract(epoch FROM clock_timestamp()))::bigint FROM versions "
                        "ON CONFLICT (agent_version) DO UPDATE SET pending=true "
                        "RETURNING agent_version) SELECT agent_version FROM inserted "
                        "ORDER BY agent_version"
                    ).format(
                        self._aggregation_table("user_playbooks"),
                        self._aggregation_table("playbook_aggregation_state"),
                        self._aggregation_table("playbook_aggregation_item"),
                        self._aggregation_table("playbook_aggregation_item"),
                        self._aggregation_table("playbook_aggregation_invalidation"),
                        self._aggregation_table("playbook_aggregation_cluster"),
                        self._aggregation_table("playbook_aggregation_state"),
                        self._aggregation_table("playbook_aggregation_state"),
                    ),
                    [limit],
                )
                versions = [str(row[0]) for row in cur.fetchall()]
            conn.commit()
        return versions

    def claim_due_playbook_aggregation(
        self,
        *,
        owner: str,
        lease_seconds: int,
        agent_version: str | None = None,
    ) -> PlaybookAggregationClaim | None:
        if not owner.strip() or lease_seconds <= 0:
            raise ValueError("claim owner and lease_seconds must be valid")
        with self._writer_connection(operation="aggregation.claim") as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        sql.SQL(
                            "SELECT * FROM {} WHERE singleton=true FOR UPDATE"
                        ).format(self._aggregation_table("playbook_aggregation_lease"))
                    )
                    lease = cur.fetchone()
                    cur.execute(
                        "SELECT floor(extract(epoch FROM clock_timestamp()))::bigint AS now"
                    )
                    now = int(cur.fetchone()["now"])
                    if (
                        lease
                        and lease["claim_expires_at"] is not None
                        and int(lease["claim_expires_at"]) > now
                    ):
                        conn.commit()
                        return None
                    if agent_version is None:
                        cur.execute(
                            sql.SQL(
                                "SELECT agent_version, state_version FROM {} "
                                "WHERE pending=true AND next_attempt_at <= %s "
                                "ORDER BY next_attempt_at, agent_version "
                                "FOR UPDATE SKIP LOCKED LIMIT 1"
                            ).format(
                                self._aggregation_table("playbook_aggregation_state")
                            ),
                            [now],
                        )
                    else:
                        cur.execute(
                            sql.SQL(
                                "SELECT agent_version, state_version FROM {} "
                                "WHERE agent_version=%s FOR UPDATE"
                            ).format(
                                self._aggregation_table("playbook_aggregation_state")
                            ),
                            [agent_version],
                        )
                    state = cur.fetchone()
                    if state is None:
                        conn.commit()
                        return None
                    fence = int(lease["claim_fence"] if lease else 0) + 1
                    expires_at = now + lease_seconds
                    cur.execute(
                        sql.SQL(
                            "UPDATE {} SET claim_owner=%s, claim_fence=%s, "
                            "claim_expires_at=%s, agent_version=%s WHERE singleton=true"
                        ).format(self._aggregation_table("playbook_aggregation_lease")),
                        [owner, fence, expires_at, state["agent_version"]],
                    )
                conn.commit()
                return PlaybookAggregationClaim(
                    agent_version=str(state["agent_version"]),
                    owner=owner,
                    fence=fence,
                    state_version=int(state["state_version"]),
                    expires_at=expires_at,
                )
            except Exception:
                conn.rollback()
                raise

    def renew_playbook_aggregation_claim(
        self, claim: PlaybookAggregationClaim, *, lease_seconds: int
    ) -> PlaybookAggregationClaim | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        with self._writer_connection(operation="aggregation.renew") as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    sql.SQL(
                        "UPDATE {} SET claim_expires_at="
                        "floor(extract(epoch FROM clock_timestamp()))::bigint+%s "
                        "WHERE singleton=true AND claim_owner=%s AND claim_fence=%s "
                        "AND agent_version=%s AND claim_expires_at > "
                        "floor(extract(epoch FROM clock_timestamp()))::bigint "
                        "RETURNING claim_expires_at"
                    ).format(self._aggregation_table("playbook_aggregation_lease")),
                    [lease_seconds, claim.owner, claim.fence, claim.agent_version],
                )
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return None
        return PlaybookAggregationClaim(
            claim.agent_version,
            claim.owner,
            claim.fence,
            claim.state_version,
            int(row["claim_expires_at"]),
        )

    def validate_playbook_aggregation_claim(
        self, claim: PlaybookAggregationClaim
    ) -> bool:
        with self._writer_connection(operation="aggregation.validate_claim") as conn:
            with conn.cursor() as cur:
                if not self._lock_live_claim(cur, claim):
                    conn.rollback()
                    return False
                cur.execute(
                    sql.SQL(
                        "SELECT 1 FROM {} WHERE agent_version=%s "
                        "AND state_version=%s FOR UPDATE"
                    ).format(self._aggregation_table("playbook_aggregation_state")),
                    [claim.agent_version, claim.state_version],
                )
                valid = cur.fetchone() is not None
            conn.rollback()
        return valid

    def _lock_live_claim(self, cur: Any, claim: PlaybookAggregationClaim) -> bool:
        cur.execute(
            sql.SQL(
                "SELECT 1 FROM {} WHERE singleton=true AND claim_owner=%s "
                "AND claim_fence=%s AND agent_version=%s AND claim_expires_at > "
                "floor(extract(epoch FROM clock_timestamp()))::bigint FOR UPDATE"
            ).format(self._aggregation_table("playbook_aggregation_lease")),
            [claim.owner, claim.fence, claim.agent_version],
        )
        return cur.fetchone() is not None

    def finish_playbook_aggregation_claim(
        self,
        claim: PlaybookAggregationClaim,
        *,
        success: bool,
        retry_after_seconds: int,
        backlog_retry_after_seconds: int,
        min_interval_seconds: int,
        backlog: PlaybookAggregationBacklog | None = None,
    ) -> bool:
        with self._writer_connection(operation="aggregation.finish") as conn:
            try:
                with conn.cursor() as cur:
                    if not self._lock_live_claim(cur, claim):
                        conn.rollback()
                        return False
                    if not success:
                        delay = retry_after_seconds
                        pending = True
                    else:
                        backlog = backlog or self.get_playbook_aggregation_backlog(
                            claim.agent_version
                        )
                        pending = backlog.pending
                        if pending:
                            delay = max(
                                backlog_retry_after_seconds,
                                backlog.continuation_delay_seconds,
                            )
                        else:
                            delay = min_interval_seconds
                    cur.execute(
                        sql.SQL(
                            "UPDATE {} SET last_success_at=CASE WHEN %s THEN "
                            "floor(extract(epoch FROM clock_timestamp()))::bigint "
                            "ELSE last_success_at END, pending=%s, next_attempt_at="
                            "floor(extract(epoch FROM clock_timestamp()))::bigint+%s, "
                            "state_version=state_version+1 WHERE agent_version=%s "
                            "AND state_version=%s"
                        ).format(self._aggregation_table("playbook_aggregation_state")),
                        [
                            success,
                            pending,
                            max(0, delay),
                            claim.agent_version,
                            claim.state_version,
                        ],
                    )
                    if cur.rowcount != 1:
                        conn.rollback()
                        return False
                    cur.execute(
                        sql.SQL(
                            "UPDATE {} SET claim_owner=NULL, claim_expires_at=NULL, "
                            "agent_version=NULL WHERE singleton=true AND claim_owner=%s "
                            "AND claim_fence=%s"
                        ).format(self._aggregation_table("playbook_aggregation_lease")),
                        [claim.owner, claim.fence],
                    )
                conn.commit()
                return True
            except Exception:
                conn.rollback()
                raise

    def stage_playbook_aggregation_intake(
        self, agent_version: str, *, limit: int, window_limit: int = 20_000
    ) -> list[int]:
        if limit <= 0 or window_limit <= 0:
            return []
        with self._writer_connection(operation="aggregation.intake") as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL(
                            "INSERT INTO {} (agent_version, pending, next_attempt_at) "
                            "VALUES (%s, true, floor(extract(epoch FROM "
                            "clock_timestamp()))::bigint) ON CONFLICT (agent_version) "
                            "DO NOTHING"
                        ).format(self._aggregation_table("playbook_aggregation_state")),
                        [agent_version],
                    )
                    cur.execute(
                        sql.SQL(
                            "SELECT intake_floor_user_playbook_id FROM {} "
                            "WHERE agent_version=%s FOR UPDATE"
                        ).format(self._aggregation_table("playbook_aggregation_state")),
                        [agent_version],
                    )
                    old_floor = int(cur.fetchone()[0])
                    cur.execute(
                        sql.SQL(
                            "SELECT p.user_playbook_id FROM {} p LEFT JOIN {} i ON "
                            "i.agent_version=%s AND i.user_playbook_id=p.user_playbook_id "
                            "WHERE p.agent_version=%s AND p.status IS NULL AND "
                            "NULLIF(btrim(p.content), '') IS NOT NULL AND "
                            "p.user_playbook_id>=%s AND (i.user_playbook_id IS NULL OR "
                            "(i.disposition='residual' AND i.cluster_id IS NULL)) "
                            "ORDER BY p.user_playbook_id DESC OFFSET %s LIMIT 1"
                        ).format(
                            self._aggregation_table("user_playbooks"),
                            self._aggregation_table("playbook_aggregation_item"),
                        ),
                        [agent_version, agent_version, old_floor, window_limit - 1],
                    )
                    cutoff = cur.fetchone()
                    intake_floor = max(
                        old_floor, int(cutoff[0]) if cutoff is not None else old_floor
                    )
                    if intake_floor != old_floor:
                        cur.execute(
                            sql.SQL(
                                "UPDATE {} SET intake_floor_user_playbook_id=%s "
                                "WHERE agent_version=%s"
                            ).format(
                                self._aggregation_table("playbook_aggregation_state")
                            ),
                            [intake_floor, agent_version],
                        )
                        cur.execute(
                            sql.SQL(
                                "UPDATE {} SET disposition='terminal_noop', "
                                "cluster_id=NULL, reason='outside_recent_clustering_window', "
                                "updated_at=floor(extract(epoch FROM clock_timestamp()))::bigint "
                                "WHERE agent_version=%s AND disposition='residual' "
                                "AND cluster_id IS NULL AND user_playbook_id<%s"
                            ).format(
                                self._aggregation_table("playbook_aggregation_item")
                            ),
                            [agent_version, intake_floor],
                        )
                    cur.execute(
                        sql.SQL(
                            "WITH candidates AS (SELECT p.user_playbook_id FROM {} p "
                            "WHERE p.agent_version=%s AND p.status IS NULL AND "
                            "NULLIF(btrim(p.content), '') IS NOT NULL AND "
                            "p.user_playbook_id>=%s AND NOT EXISTS (SELECT 1 FROM {} i "
                            "WHERE i.agent_version=%s AND "
                            "i.user_playbook_id=p.user_playbook_id) ORDER BY "
                            "p.user_playbook_id DESC LIMIT %s), inserted AS (INSERT INTO {} "
                            "(agent_version, user_playbook_id, disposition, reason) SELECT "
                            "%s, user_playbook_id, 'residual', 'new' FROM candidates "
                            "ON CONFLICT DO NOTHING RETURNING user_playbook_id) SELECT "
                            "user_playbook_id FROM inserted ORDER BY user_playbook_id DESC"
                        ).format(
                            self._aggregation_table("user_playbooks"),
                            self._aggregation_table("playbook_aggregation_item"),
                            self._aggregation_table("playbook_aggregation_item"),
                        ),
                        [
                            agent_version,
                            intake_floor,
                            agent_version,
                            limit,
                            agent_version,
                        ],
                    )
                    ids = [int(row[0]) for row in cur.fetchall()]
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return ids

    def get_playbook_aggregation_bootstrap_status(self, agent_version: str) -> str:
        with self._writer_connection(operation="aggregation.bootstrap_status") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT bootstrap_status FROM {} WHERE agent_version=%s"
                    ).format(self._aggregation_table("playbook_aggregation_state")),
                    [agent_version],
                )
                row = cur.fetchone()
            conn.rollback()
        return str(row[0]) if row is not None else "pending"

    def set_playbook_aggregation_bootstrap_status(
        self, agent_version: str, status: str
    ) -> None:
        if status not in {"pending", "complete"}:
            raise ValueError("invalid aggregation bootstrap status")
        with self._writer_connection(
            operation="aggregation.bootstrap_complete"
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "INSERT INTO {} (agent_version, bootstrap_status, pending, "
                        "next_attempt_at) VALUES (%s, %s, true, floor(extract(epoch "
                        "FROM clock_timestamp()))::bigint) ON CONFLICT (agent_version) "
                        "DO UPDATE SET bootstrap_status=excluded.bootstrap_status"
                    ).format(self._aggregation_table("playbook_aggregation_state")),
                    [agent_version, status],
                )
            conn.commit()

    def get_playbook_aggregation_cluster_rebuild_cursor(
        self, cluster_id: str
    ) -> tuple[int, str] | None:
        with self._writer_connection(operation="aggregation.rebuild_cursor") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT rebuild_cursor, state FROM {} WHERE cluster_id=%s::uuid"
                    ).format(self._aggregation_table("playbook_aggregation_cluster")),
                    [cluster_id],
                )
                row = cur.fetchone()
            conn.rollback()
        if row is None:
            return None
        return int(row[0]), str(row[1])

    def adopt_legacy_playbook_aggregation_cluster_page(
        self,
        *,
        cluster_id: str,
        agent_version: str,
        agent_playbook_id: int,
        centroid_embedding: list[float],
        member_embeddings: list[tuple[int, list[float]]],
        embedding_model: str,
        embedding_dimension: int,
        rebuild_cursor: int,
        complete: bool,
    ) -> None:
        if len(centroid_embedding) != embedding_dimension or any(
            len(value) != embedding_dimension for _, value in member_embeddings
        ):
            raise ValueError("legacy cluster embedding dimension changed")
        with self._writer_connection(operation="aggregation.adopt_legacy") as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL(
                            "INSERT INTO {} (cluster_id, agent_version, "
                            "agent_playbook_id, member_count, embedding_model, "
                            "embedding_dimension, state) VALUES (%s::uuid, %s, %s, "
                            "0, %s, %s, 'rebuilding') ON CONFLICT (cluster_id) DO NOTHING"
                        ).format(
                            self._aggregation_table("playbook_aggregation_cluster")
                        ),
                        [
                            cluster_id,
                            agent_version,
                            agent_playbook_id,
                            embedding_model,
                            embedding_dimension,
                        ],
                    )
                    cur.execute(
                        sql.SQL(
                            "SELECT vector_sum::text, member_count, state, "
                            "embedding_model, embedding_dimension FROM {} "
                            "WHERE cluster_id=%s::uuid FOR UPDATE"
                        ).format(
                            self._aggregation_table("playbook_aggregation_cluster")
                        ),
                        [cluster_id],
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise RuntimeError("legacy aggregation cluster was not created")
                    if str(row[2]) == "active":
                        conn.rollback()
                        return
                    if row[3] != embedding_model or int(row[4]) != embedding_dimension:
                        raise RuntimeError(
                            "legacy cluster embedding provenance changed"
                        )
                    vector_sum = (
                        [float(value) for value in str(row[0]).strip("[]").split(",")]
                        if row[0]
                        else [0.0] * embedding_dimension
                    )
                    member_count = int(row[1])
                    for user_playbook_id, embedding in member_embeddings:
                        cur.execute(
                            sql.SQL(
                                "INSERT INTO {} (agent_version, user_playbook_id, "
                                "disposition, cluster_id, reason) VALUES (%s, %s, "
                                "'cluster_member', %s::uuid, 'legacy_adopted') "
                                "ON CONFLICT DO NOTHING"
                            ).format(
                                self._aggregation_table("playbook_aggregation_item")
                            ),
                            [agent_version, user_playbook_id, cluster_id],
                        )
                        if cur.rowcount == 1:
                            vector_sum = [
                                left + right
                                for left, right in zip(
                                    vector_sum, embedding, strict=True
                                )
                            ]
                            member_count += 1
                    centroid = centroid_embedding if complete and member_count else None
                    cur.execute(
                        sql.SQL(
                            "UPDATE {} SET vector_sum=%s::public.vector, "
                            "centroid=%s::public.vector, member_count=%s, "
                            "rebuild_cursor=%s, state=%s WHERE cluster_id=%s::uuid"
                        ).format(
                            self._aggregation_table("playbook_aggregation_cluster")
                        ),
                        [
                            None if complete else json.dumps(vector_sum),
                            json.dumps(centroid) if centroid is not None else None,
                            member_count,
                            rebuild_cursor,
                            "active" if complete else "rebuilding",
                            cluster_id,
                        ],
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def reset_playbook_aggregation_version(self, agent_version: str) -> None:
        with self._writer_connection(operation="aggregation.reset_version") as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("DELETE FROM {} WHERE agent_version=%s").format(
                            self._aggregation_table("playbook_aggregation_item")
                        ),
                        [agent_version],
                    )
                    cur.execute(
                        sql.SQL("DELETE FROM {} WHERE agent_version=%s").format(
                            self._aggregation_table("playbook_aggregation_cluster")
                        ),
                        [agent_version],
                    )
                    cur.execute(
                        sql.SQL(
                            "UPDATE {} SET bootstrap_status='complete', "
                            "intake_floor_user_playbook_id=0 "
                            "WHERE agent_version=%s"
                        ).format(self._aggregation_table("playbook_aggregation_state")),
                        [agent_version],
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def capture_playbook_aggregation_rerun_snapshot(
        self, agent_version: str, *, limit: int
    ) -> PlaybookAggregationRerunSnapshot:
        if limit <= 0:
            return PlaybookAggregationRerunSnapshot((), (), None, None)
        with self._writer_connection(operation="aggregation.rerun_snapshot") as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                    )
                    cur.execute(
                        sql.SQL(
                            "SELECT {} FROM {} WHERE agent_version=%s "
                            "AND status IS NULL ORDER BY user_playbook_id DESC LIMIT %s"
                        ).format(
                            sql.SQL(_USER_PLAYBOOK_COLUMNS_WITH_EMBEDDING),
                            self._aggregation_table("user_playbooks"),
                        ),
                        [agent_version, limit],
                    )
                    rows = cur.fetchall()
                    cur.execute(
                        sql.SQL(
                            "SELECT invalidation_id FROM {} WHERE agent_version=%s "
                            "AND processed_at IS NULL ORDER BY invalidation_id LIMIT %s"
                        ).format(
                            self._aggregation_table("playbook_aggregation_invalidation")
                        ),
                        [agent_version, limit],
                    )
                    invalidation_ids = tuple(
                        int(row["invalidation_id"]) for row in cur.fetchall()
                    )
                conn.rollback()
            except Exception:
                conn.rollback()
                raise
        normalized_rows = [dict(row) for row in rows]
        for row in normalized_rows:
            created_at = row.get("created_at")
            if created_at is not None and not isinstance(created_at, str):
                row["created_at"] = created_at.isoformat()
        playbooks = tuple(
            self._row_to_user_playbook(row, include_embedding=True)
            for row in normalized_rows
        )
        return PlaybookAggregationRerunSnapshot(
            user_playbooks=playbooks,
            invalidation_ids=invalidation_ids,
            user_high_watermark=(
                max(item.user_playbook_id for item in playbooks) if playbooks else None
            ),
            invalidation_high_watermark=(
                max(invalidation_ids) if invalidation_ids else None
            ),
        )

    def stage_playbook_aggregation_snapshot(
        self, agent_version: str, user_playbook_ids: list[int]
    ) -> None:
        if not user_playbook_ids:
            return
        with self._writer_connection(operation="aggregation.stage_snapshot") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "INSERT INTO {} (agent_version, user_playbook_id, "
                        "disposition, reason) SELECT %s, item_id, 'residual', "
                        "'full_rerun_snapshot' FROM unnest(%s::bigint[]) AS item_id "
                        "ON CONFLICT DO NOTHING"
                    ).format(self._aggregation_table("playbook_aggregation_item")),
                    [agent_version, user_playbook_ids],
                )
            conn.commit()

    def mark_playbook_aggregation_invalidations_processed(
        self,
        claim: PlaybookAggregationClaim,
        invalidation_ids: list[int],
    ) -> bool:
        if not invalidation_ids:
            return True
        with self._writer_connection(
            operation="aggregation.complete_rerun_invalidations"
        ) as conn:
            try:
                with conn.cursor() as cur:
                    if not self._lock_live_claim(cur, claim):
                        conn.rollback()
                        return False
                    cur.execute(
                        sql.SQL(
                            "UPDATE {} SET processed_at=floor(extract(epoch FROM "
                            "clock_timestamp()))::bigint WHERE agent_version=%s "
                            "AND invalidation_id=ANY(%s) AND processed_at IS NULL"
                        ).format(
                            self._aggregation_table("playbook_aggregation_invalidation")
                        ),
                        [claim.agent_version, invalidation_ids],
                    )
                conn.commit()
                return True
            except Exception:
                conn.rollback()
                raise

    def get_playbook_aggregation_residual_ids(
        self, agent_version: str, *, limit: int
    ) -> list[int]:
        if limit <= 0:
            return []
        with self._writer_connection(operation="aggregation.residuals") as conn:
            with conn.cursor() as cur:
                fresh_quota = (limit + 1) // 2
                retry_quota = limit - fresh_quota
                cur.execute(
                    sql.SQL(
                        "WITH fresh AS MATERIALIZED (SELECT i.user_playbook_id FROM {} i "
                        "WHERE i.agent_version=%s AND i.disposition='residual' "
                        "AND i.attempt_count=0 AND NOT EXISTS (SELECT 1 FROM {} c "
                        "WHERE c.agent_version=i.agent_version AND "
                        "c.cluster_id=i.cluster_id AND c.state='rebuilding' AND "
                        "c.rebuild_next_attempt_at>floor(extract(epoch FROM "
                        "clock_timestamp()))::bigint) ORDER BY i.user_playbook_id "
                        "FOR UPDATE SKIP LOCKED LIMIT %s), retries AS MATERIALIZED ("
                        "SELECT i.user_playbook_id FROM {} i WHERE i.agent_version=%s "
                        "AND i.disposition='residual' AND i.attempt_count>0 AND "
                        "i.last_attempt_at+LEAST(%s, %s*(1::bigint << LEAST("
                        "GREATEST(i.attempt_count-1, 0), 6))) <= floor(extract(epoch "
                        "FROM clock_timestamp()))::bigint AND NOT EXISTS (SELECT 1 "
                        "FROM {} c WHERE c.agent_version=i.agent_version AND "
                        "c.cluster_id=i.cluster_id AND c.state='rebuilding' AND "
                        "c.rebuild_next_attempt_at>floor(extract(epoch FROM "
                        "clock_timestamp()))::bigint) ORDER BY i.last_attempt_at, "
                        "i.user_playbook_id FOR UPDATE SKIP LOCKED LIMIT %s), "
                        "quota AS MATERIALIZED (SELECT user_playbook_id FROM fresh "
                        "UNION ALL SELECT user_playbook_id FROM retries), "
                        "fill AS MATERIALIZED (SELECT i.user_playbook_id FROM {} i "
                        "WHERE i.agent_version=%s AND i.disposition='residual' AND "
                        "(i.attempt_count=0 OR i.last_attempt_at IS NULL OR "
                        "i.last_attempt_at+LEAST(%s, %s*(1::bigint << LEAST("
                        "GREATEST(i.attempt_count-1, 0), 6))) <= floor(extract(epoch "
                        "FROM clock_timestamp()))::bigint) AND NOT EXISTS (SELECT 1 "
                        "FROM quota q WHERE q.user_playbook_id=i.user_playbook_id) "
                        "AND NOT EXISTS (SELECT 1 FROM {} c WHERE "
                        "c.agent_version=i.agent_version AND c.cluster_id=i.cluster_id "
                        "AND c.state='rebuilding' AND c.rebuild_next_attempt_at>"
                        "floor(extract(epoch FROM clock_timestamp()))::bigint) "
                        "ORDER BY COALESCE(i.last_attempt_at, 0), i.user_playbook_id "
                        "FOR UPDATE SKIP LOCKED LIMIT %s), selected AS (SELECT "
                        "user_playbook_id FROM quota UNION ALL SELECT user_playbook_id "
                        "FROM fill LIMIT %s) UPDATE {} i SET attempt_count="
                        "attempt_count+1, last_attempt_at=floor(extract(epoch FROM "
                        "clock_timestamp()))::bigint, updated_at=floor(extract(epoch "
                        "FROM clock_timestamp()))::bigint FROM selected s WHERE "
                        "i.agent_version=%s AND i.user_playbook_id=s.user_playbook_id "
                        "RETURNING i.user_playbook_id"
                    ).format(
                        self._aggregation_table("playbook_aggregation_item"),
                        self._aggregation_table("playbook_aggregation_cluster"),
                        self._aggregation_table("playbook_aggregation_item"),
                        self._aggregation_table("playbook_aggregation_cluster"),
                        self._aggregation_table("playbook_aggregation_item"),
                        self._aggregation_table("playbook_aggregation_cluster"),
                        self._aggregation_table("playbook_aggregation_item"),
                    ),
                    [
                        agent_version,
                        fresh_quota,
                        agent_version,
                        AGGREGATION_RETRY_MAX_SECONDS,
                        AGGREGATION_RETRY_BASE_SECONDS,
                        retry_quota,
                        agent_version,
                        AGGREGATION_RETRY_MAX_SECONDS,
                        AGGREGATION_RETRY_BASE_SECONDS,
                        limit,
                        limit,
                        agent_version,
                    ],
                )
                ids = [int(row[0]) for row in cur.fetchall()]
            conn.commit()
        return sorted(ids)

    def set_playbook_aggregation_disposition(
        self,
        agent_version: str,
        user_playbook_ids: list[int],
        *,
        disposition: AggregationDisposition,
        cluster_id: str | None = None,
        reason: str | None = None,
    ) -> None:
        if not user_playbook_ids:
            return
        with self._writer_connection(operation="aggregation.disposition") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "UPDATE {} SET disposition=%s, cluster_id=%s, reason=%s, "
                        "updated_at=floor(extract(epoch FROM clock_timestamp()))::bigint "
                        "WHERE agent_version=%s AND user_playbook_id=ANY(%s)"
                    ).format(self._aggregation_table("playbook_aggregation_item")),
                    [disposition, cluster_id, reason, agent_version, user_playbook_ids],
                )
            conn.commit()

    def get_playbook_aggregation_backlog(
        self, agent_version: str
    ) -> PlaybookAggregationBacklog:
        with self._writer_connection(operation="aggregation.backlog") as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT (SELECT count(*) FROM {} p WHERE p.agent_version=%s "
                        "AND p.status IS NULL AND NULLIF(btrim(p.content), '') IS NOT NULL "
                        "AND p.user_playbook_id>=COALESCE((SELECT "
                        "intake_floor_user_playbook_id FROM {} s WHERE "
                        "s.agent_version=%s), 0) "
                        "AND NOT EXISTS (SELECT 1 FROM {} i WHERE i.agent_version=%s "
                        "AND i.user_playbook_id=p.user_playbook_id)) AS undisposed, "
                        "(SELECT count(*) FROM {} WHERE agent_version=%s "
                        "AND disposition='residual') AS residual, "
                        "(SELECT count(*) FROM {} WHERE agent_version=%s "
                        "AND processed_at IS NULL) AS invalidations, "
                        "(SELECT floor(extract(epoch FROM clock_timestamp()))::bigint-"
                        "min(created_at) FROM {} WHERE agent_version=%s "
                        "AND disposition='residual') AS oldest_residual_age, "
                        "(SELECT count(*) FROM {} WHERE agent_version=%s "
                        "AND (dirty=true OR state='rebuilding')) AS dirty_repairs, "
                        "(SELECT GREATEST(0, COALESCE(min(CASE WHEN c.state='rebuilding' "
                        "THEN c.rebuild_next_attempt_at WHEN i.attempt_count=0 "
                        "OR i.last_attempt_at IS NULL THEN floor(extract(epoch FROM "
                        "clock_timestamp()))::bigint ELSE i.last_attempt_at+LEAST(%s, "
                        "%s*(1::bigint << LEAST(GREATEST(i.attempt_count-1, 0), 6))) "
                        "END)-floor(extract(epoch FROM clock_timestamp()))::bigint, 0)) "
                        "FROM {} i LEFT JOIN {} c ON c.agent_version=i.agent_version "
                        "AND c.cluster_id=i.cluster_id WHERE i.agent_version=%s AND "
                        "i.disposition='residual') "
                        "AS residual_retry_after, (SELECT GREATEST(0, COALESCE("
                        "min(rebuild_next_attempt_at)-floor(extract(epoch FROM "
                        "clock_timestamp()))::bigint, 0)) FROM {} WHERE "
                        "agent_version=%s AND state='rebuilding') AS repair_retry_after"
                    ).format(
                        self._aggregation_table("user_playbooks"),
                        self._aggregation_table("playbook_aggregation_state"),
                        self._aggregation_table("playbook_aggregation_item"),
                        self._aggregation_table("playbook_aggregation_item"),
                        self._aggregation_table("playbook_aggregation_invalidation"),
                        self._aggregation_table("playbook_aggregation_item"),
                        self._aggregation_table("playbook_aggregation_cluster"),
                        self._aggregation_table("playbook_aggregation_item"),
                        self._aggregation_table("playbook_aggregation_cluster"),
                        self._aggregation_table("playbook_aggregation_cluster"),
                    ),
                    [
                        agent_version,
                        agent_version,
                        agent_version,
                        agent_version,
                        agent_version,
                        agent_version,
                        agent_version,
                        AGGREGATION_RETRY_MAX_SECONDS,
                        AGGREGATION_RETRY_BASE_SECONDS,
                        agent_version,
                        agent_version,
                    ],
                )
                row = cur.fetchone()
            conn.rollback()
        return PlaybookAggregationBacklog(
            int(row["undisposed"]),
            int(row["residual"]),
            int(row["invalidations"]),
            (
                int(row["oldest_residual_age"])
                if row["oldest_residual_age"] is not None
                else None
            ),
            int(row["dirty_repairs"]),
            int(row["residual_retry_after"]),
            int(row["repair_retry_after"]),
        )

    def append_playbook_aggregation_invalidation(
        self,
        *,
        agent_version: str,
        operation: str,
        entity_id: int,
        source_ids: list[int] | None = None,
    ) -> None:
        with self._writer_connection(operation="aggregation.invalidate") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "INSERT INTO {} (agent_version, operation, entity_id, source_ids) "
                        "VALUES (%s, %s, %s, %s)"
                    ).format(
                        self._aggregation_table("playbook_aggregation_invalidation")
                    ),
                    [agent_version, operation, entity_id, Json(source_ids or [])],
                )
                cur.execute(
                    sql.SQL(
                        "INSERT INTO {} (agent_version, pending, next_attempt_at) "
                        "VALUES (%s, true, floor(extract(epoch FROM clock_timestamp()))::bigint) "
                        "ON CONFLICT (agent_version) DO UPDATE SET pending=true"
                    ).format(self._aggregation_table("playbook_aggregation_state")),
                    [agent_version],
                )
            conn.commit()

    def get_playbook_aggregation_invalidations(
        self, agent_version: str, *, limit: int
    ) -> list[PlaybookAggregationInvalidation]:
        if limit <= 0:
            return []
        with self._writer_connection(operation="aggregation.invalidations") as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT invalidation_id, operation, entity_id, source_ids FROM {} "
                        "WHERE agent_version=%s AND processed_at IS NULL "
                        "ORDER BY invalidation_id LIMIT %s"
                    ).format(
                        self._aggregation_table("playbook_aggregation_invalidation")
                    ),
                    [agent_version, limit],
                )
                rows = cur.fetchall()
            conn.rollback()
        return [
            PlaybookAggregationInvalidation(
                int(row["invalidation_id"]),
                agent_version,
                str(row["operation"]),
                int(row["entity_id"]),
                tuple(int(value) for value in (row["source_ids"] or [])),
            )
            for row in rows
        ]

    def apply_playbook_aggregation_invalidations(
        self, claim: PlaybookAggregationClaim, invalidation_ids: list[int]
    ) -> bool:
        if not invalidation_ids:
            return True
        with self._writer_connection(
            operation="aggregation.apply_invalidations"
        ) as conn:
            try:
                with conn.cursor() as cur:
                    if not self._lock_live_claim(cur, claim):
                        conn.rollback()
                        return False
                    cur.execute(
                        sql.SQL(
                            "WITH events AS MATERIALIZED (SELECT operation, entity_id, "
                            "source_ids "
                            "FROM {} WHERE agent_version=%s AND invalidation_id=ANY(%s) "
                            "AND processed_at IS NULL), affected AS MATERIALIZED ("
                            "SELECT entity_id FROM events UNION SELECT "
                            "jsonb_array_elements_text(source_ids)::bigint FROM events), "
                            "revision_clusters AS MATERIALIZED (SELECT e.entity_id, "
                            "(array_agg(DISTINCT i.cluster_id))[1] AS cluster_id FROM events e "
                            "CROSS JOIN LATERAL jsonb_array_elements_text(e.source_ids) s(id) "
                            "JOIN {} i ON i.agent_version=%s AND "
                            "i.user_playbook_id=s.id::bigint WHERE e.operation='revise' "
                            "AND i.cluster_id IS NOT NULL GROUP BY e.entity_id "
                            "HAVING count(DISTINCT i.cluster_id)=1), "
                            "clusters AS MATERIALIZED (SELECT DISTINCT i.cluster_id FROM {} i "
                            "JOIN affected a ON a.entity_id=i.user_playbook_id WHERE "
                            "i.agent_version=%s AND i.cluster_id IS NOT NULL), "
                            "reset_members AS (UPDATE {} i SET disposition='residual', "
                            "reason='cluster_invalidated', attempt_count=0, last_attempt_at=NULL, "
                            "updated_at=floor(extract(epoch FROM clock_timestamp()))::bigint "
                            "FROM clusters c WHERE i.agent_version=%s AND "
                            "i.cluster_id=c.cluster_id AND NOT EXISTS (SELECT 1 FROM affected a "
                            "WHERE a.entity_id=i.user_playbook_id) RETURNING 1), "
                            "mark_clusters AS (UPDATE {} c SET state='rebuilding', dirty=true, "
                            "centroid=NULL, vector_sum=NULL, member_count=0, rebuild_cursor=0, "
                            "rebuild_attempt_count=0, rebuild_next_attempt_at=0 "
                            "FROM clusters x WHERE c.agent_version=%s AND "
                            "c.cluster_id=x.cluster_id RETURNING 1), remove_sources AS ("
                            "DELETE FROM {} i USING affected a WHERE i.agent_version=%s AND "
                            "i.user_playbook_id=a.entity_id RETURNING 1), restore_revisions AS ("
                            "INSERT INTO {} (agent_version, user_playbook_id, disposition, "
                            "cluster_id, reason) SELECT %s, p.user_playbook_id, 'residual', "
                            "r.cluster_id, 'revision_rebuild' FROM revision_clusters r JOIN {} p "
                            "ON p.user_playbook_id=r.entity_id WHERE p.agent_version=%s "
                            "AND p.status IS NULL AND btrim(p.content) <> '' AND "
                            "(SELECT count(*) FROM remove_sources) >= 0 ON CONFLICT "
                            "(agent_version, user_playbook_id) DO UPDATE SET "
                            "disposition='residual', cluster_id=excluded.cluster_id, "
                            "reason=excluded.reason, attempt_count=0, last_attempt_at=NULL, "
                            "updated_at=floor(extract(epoch FROM clock_timestamp()))::bigint "
                            "RETURNING 1), complete AS ("
                            "UPDATE {} SET processed_at=floor(extract(epoch FROM "
                            "clock_timestamp()))::bigint WHERE agent_version=%s AND "
                            "invalidation_id=ANY(%s) AND processed_at IS NULL RETURNING 1) "
                            "SELECT (SELECT count(*) FROM reset_members), "
                            "(SELECT count(*) FROM mark_clusters), "
                            "(SELECT count(*) FROM remove_sources), "
                            "(SELECT count(*) FROM complete)"
                        ).format(
                            self._aggregation_table(
                                "playbook_aggregation_invalidation"
                            ),
                            self._aggregation_table("playbook_aggregation_item"),
                            self._aggregation_table("playbook_aggregation_item"),
                            self._aggregation_table("playbook_aggregation_item"),
                            self._aggregation_table("playbook_aggregation_cluster"),
                            self._aggregation_table("playbook_aggregation_item"),
                            self._aggregation_table("playbook_aggregation_item"),
                            self._aggregation_table("user_playbooks"),
                            self._aggregation_table(
                                "playbook_aggregation_invalidation"
                            ),
                        ),
                        [
                            claim.agent_version,
                            invalidation_ids,
                            claim.agent_version,
                            claim.agent_version,
                            claim.agent_version,
                            claim.agent_version,
                            claim.agent_version,
                            claim.agent_version,
                            claim.agent_version,
                            claim.agent_version,
                            invalidation_ids,
                        ],
                    )
                conn.commit()
                return True
            except Exception:
                conn.rollback()
                raise

    def get_playbook_aggregation_rebuild_cluster_ids(
        self, agent_version: str, user_playbook_ids: list[int]
    ) -> dict[int, str]:
        if not user_playbook_ids:
            return {}
        with self._writer_connection(operation="aggregation.rebuild_groups") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT i.user_playbook_id, i.cluster_id::text FROM {} i "
                        "JOIN {} c ON c.cluster_id=i.cluster_id WHERE "
                        "i.agent_version=%s AND i.user_playbook_id=ANY(%s) "
                        "AND i.disposition='residual' AND c.state='rebuilding' "
                        "AND c.rebuild_next_attempt_at<=floor(extract(epoch FROM "
                        "clock_timestamp()))::bigint"
                    ).format(
                        self._aggregation_table("playbook_aggregation_item"),
                        self._aggregation_table("playbook_aggregation_cluster"),
                    ),
                    [agent_version, user_playbook_ids],
                )
                rows = cur.fetchall()
            conn.rollback()
        return {int(row[0]): str(row[1]) for row in rows}

    def get_playbook_aggregation_rebuild_samples(
        self, agent_version: str, cluster_ids: list[str], *, limit_per_cluster: int
    ) -> list[PlaybookAggregationRebuildSample]:
        if not cluster_ids or limit_per_cluster <= 0:
            return []
        with self._writer_connection(operation="aggregation.rebuild_samples") as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    sql.SQL(
                        "SELECT c.cluster_id::text, c.agent_playbook_id, "
                        "recent.user_playbook_id FROM unnest(%s::uuid[]) AS requested("
                        "cluster_id) JOIN {} c ON c.cluster_id=requested.cluster_id "
                        "JOIN LATERAL (SELECT p.user_playbook_id, p.created_at FROM {} p "
                        "WHERE p.agent_version=%s AND NULLIF(btrim(p.agent_version), '') "
                        "IS NOT NULL AND p.status IS NULL AND NULLIF(btrim(p.content), '') "
                        "IS NOT NULL AND EXISTS (SELECT 1 FROM {} i WHERE "
                        "i.agent_version=%s AND i.user_playbook_id=p.user_playbook_id "
                        "AND i.cluster_id=c.cluster_id AND i.disposition='residual') "
                        "ORDER BY p.created_at DESC, p.user_playbook_id DESC LIMIT %s) "
                        "recent "
                        "ON true WHERE c.agent_version=%s AND c.state='rebuilding' "
                        "AND c.agent_playbook_id IS NOT NULL AND "
                        "c.rebuild_next_attempt_at<=floor(extract(epoch FROM "
                        "clock_timestamp()))::bigint ORDER BY c.cluster_id, "
                        "recent.created_at DESC, recent.user_playbook_id DESC"
                    ).format(
                        self._aggregation_table("playbook_aggregation_cluster"),
                        self._aggregation_table("user_playbooks"),
                        self._aggregation_table("playbook_aggregation_item"),
                    ),
                    [
                        cluster_ids,
                        agent_version,
                        agent_version,
                        limit_per_cluster,
                        agent_version,
                    ],
                )
                rows = cur.fetchall()
            conn.rollback()
        grouped: dict[str, list[Any]] = {}
        for row in rows:
            grouped.setdefault(str(row["cluster_id"]), []).append(row)
        return [
            PlaybookAggregationRebuildSample(
                cluster_id=cluster_id,
                agent_playbook_id=int(items[0]["agent_playbook_id"]),
                member_ids=tuple(int(item["user_playbook_id"]) for item in items),
            )
            for cluster_id, items in grouped.items()
        ]

    def defer_playbook_aggregation_cluster_rebuild(
        self,
        *,
        cluster_id: str,
        agent_version: str,
        expected_agent_playbook_id: int,
        reason: str,
    ) -> None:
        with self._writer_connection(operation="aggregation.defer_rebuild") as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL(
                            "UPDATE {} SET rebuild_attempt_count="
                            "rebuild_attempt_count+1, rebuild_next_attempt_at="
                            "floor(extract(epoch FROM clock_timestamp()))::bigint+"
                            "LEAST(%s, %s*(1::bigint << LEAST(rebuild_attempt_count, 6))) "
                            "WHERE cluster_id=%s::uuid AND agent_version=%s AND "
                            "state='rebuilding' AND agent_playbook_id=%s"
                        ).format(
                            self._aggregation_table("playbook_aggregation_cluster")
                        ),
                        [
                            AGGREGATION_RETRY_MAX_SECONDS,
                            AGGREGATION_RETRY_BASE_SECONDS,
                            cluster_id,
                            agent_version,
                            expected_agent_playbook_id,
                        ],
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError("aggregation rebuilding cluster changed")
                    cur.execute(
                        sql.SQL(
                            "UPDATE {} SET reason=%s, attempt_count=0, "
                            "last_attempt_at=NULL, updated_at=floor(extract(epoch "
                            "FROM clock_timestamp()))::bigint WHERE agent_version=%s "
                            "AND cluster_id=%s::uuid AND disposition='residual'"
                        ).format(self._aggregation_table("playbook_aggregation_item")),
                        [reason, agent_version, cluster_id],
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def complete_playbook_aggregation_cluster_rebuild(
        self,
        *,
        cluster_id: str,
        agent_version: str,
        expected_agent_playbook_id: int,
        replacement_agent_playbook_id: int,
        centroid_embedding: list[float],
        embedding_model: str,
    ) -> int:
        if not centroid_embedding:
            raise ValueError("cluster centroid embedding must be non-empty")
        with self._writer_connection(operation="aggregation.complete_rebuild") as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL(
                            "SELECT embedding_dimension FROM {} WHERE "
                            "cluster_id=%s::uuid AND agent_version=%s AND "
                            "state='rebuilding' AND agent_playbook_id=%s FOR UPDATE"
                        ).format(
                            self._aggregation_table("playbook_aggregation_cluster")
                        ),
                        [cluster_id, agent_version, expected_agent_playbook_id],
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise RuntimeError("aggregation rebuilding cluster changed")
                    if int(row[0]) != len(centroid_embedding):
                        raise ValueError("aggregation embedding dimension changed")
                    cur.execute(
                        sql.SQL(
                            "UPDATE {} SET disposition='cluster_member', "
                            "reason='rebuild_complete', updated_at=floor(extract(epoch "
                            "FROM clock_timestamp()))::bigint WHERE agent_version=%s "
                            "AND cluster_id=%s::uuid AND disposition='residual'"
                        ).format(self._aggregation_table("playbook_aggregation_item")),
                        [agent_version, cluster_id],
                    )
                    member_count = int(cur.rowcount)
                    if member_count <= 0:
                        raise RuntimeError(
                            "aggregation rebuilding cluster has no members"
                        )
                    cur.execute(
                        sql.SQL(
                            "UPDATE {} SET agent_playbook_id=%s, "
                            "centroid=%s::public.vector, vector_sum=NULL, "
                            "member_count=%s, embedding_model=%s, state='active', "
                            "dirty=false, rebuild_cursor=0, rebuild_attempt_count=0, "
                            "rebuild_next_attempt_at=0 WHERE cluster_id=%s::uuid "
                            "AND agent_version=%s AND state='rebuilding' AND "
                            "agent_playbook_id=%s"
                        ).format(
                            self._aggregation_table("playbook_aggregation_cluster")
                        ),
                        [
                            replacement_agent_playbook_id,
                            json.dumps(centroid_embedding),
                            member_count,
                            embedding_model,
                            cluster_id,
                            agent_version,
                            expected_agent_playbook_id,
                        ],
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError("aggregation rebuilding cluster changed")
                conn.commit()
                return member_count
            except Exception:
                conn.rollback()
                raise

    def discard_playbook_aggregation_cluster_rebuild(
        self,
        *,
        cluster_id: str,
        agent_version: str,
        expected_agent_playbook_id: int,
        reason: str,
    ) -> int:
        with self._writer_connection(operation="aggregation.discard_rebuild") as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL(
                            "SELECT 1 FROM {} WHERE cluster_id=%s::uuid AND "
                            "agent_version=%s AND state='rebuilding' AND "
                            "agent_playbook_id=%s FOR UPDATE"
                        ).format(
                            self._aggregation_table("playbook_aggregation_cluster")
                        ),
                        [cluster_id, agent_version, expected_agent_playbook_id],
                    )
                    if cur.fetchone() is None:
                        raise RuntimeError("aggregation rebuilding cluster changed")
                    cur.execute(
                        sql.SQL(
                            "UPDATE {} SET disposition='terminal_noop', cluster_id=NULL, "
                            "reason=%s, updated_at=floor(extract(epoch FROM "
                            "clock_timestamp()))::bigint WHERE agent_version=%s AND "
                            "cluster_id=%s::uuid AND disposition='residual'"
                        ).format(self._aggregation_table("playbook_aggregation_item")),
                        [reason, agent_version, cluster_id],
                    )
                    member_count = int(cur.rowcount)
                    cur.execute(
                        sql.SQL(
                            "DELETE FROM {} WHERE cluster_id=%s::uuid AND "
                            "agent_version=%s AND state='rebuilding' AND "
                            "agent_playbook_id=%s"
                        ).format(
                            self._aggregation_table("playbook_aggregation_cluster")
                        ),
                        [cluster_id, agent_version, expected_agent_playbook_id],
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError("aggregation rebuilding cluster changed")
                conn.commit()
                return member_count
            except Exception:
                conn.rollback()
                raise

    def delete_orphaned_playbook_aggregation_clusters(
        self, agent_version: str
    ) -> list[int]:
        with self._writer_connection(operation="aggregation.cleanup_clusters") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "DELETE FROM {} c WHERE c.agent_version=%s "
                        "AND c.state='rebuilding' AND NOT EXISTS (SELECT 1 FROM {} i "
                        "WHERE i.cluster_id=c.cluster_id) RETURNING c.agent_playbook_id"
                    ).format(
                        self._aggregation_table("playbook_aggregation_cluster"),
                        self._aggregation_table("playbook_aggregation_item"),
                    ),
                    [agent_version],
                )
                agent_ids = [
                    int(row[0]) for row in cur.fetchall() if row[0] is not None
                ]
            conn.commit()
        return agent_ids

    def find_nearest_playbook_aggregation_clusters(
        self,
        agent_version: str,
        candidates: list[tuple[int, list[float]]],
        *,
        embedding_model: str,
        limit: int,
    ) -> dict[int, PlaybookAggregationClusterMatch]:
        if limit <= 0 or not candidates:
            return {}
        candidate_ids = [item_id for item_id, _embedding in candidates]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("aggregation candidate IDs must be unique")
        dimensions = {len(embedding) for _item_id, embedding in candidates}
        if len(dimensions) != 1:
            raise ValueError(
                "aggregation candidate embeddings must share one dimension"
            )
        dimension = dimensions.pop()
        payload = Json(
            [
                {"user_playbook_id": item_id, "embedding": json.dumps(embedding)}
                for item_id, embedding in candidates
            ]
        )
        with self._writer_connection(operation="aggregation.nearest_cluster") as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    sql.SQL(
                        "WITH candidates AS (SELECT * FROM jsonb_to_recordset(%s::jsonb) "
                        "AS value(user_playbook_id bigint, embedding text)) "
                        "SELECT candidates.user_playbook_id, nearest.cluster_id::text, "
                        "nearest.similarity, nearest.agent_playbook_id FROM candidates "
                        "LEFT JOIN LATERAL (SELECT cluster_id, agent_playbook_id, "
                        "1-(centroid <=> candidates.embedding::public.vector) "
                        "AS similarity FROM {} WHERE agent_version=%s AND state='active' "
                        "AND embedding_model=%s AND embedding_dimension=%s "
                        "AND centroid IS NOT NULL AND agent_playbook_id IS NOT NULL "
                        "ORDER BY centroid <=> "
                        "candidates.embedding::public.vector, cluster_id LIMIT 1"
                        ") nearest ON true ORDER BY candidates.user_playbook_id"
                    ).format(self._aggregation_table("playbook_aggregation_cluster")),
                    [
                        payload,
                        agent_version,
                        embedding_model,
                        dimension,
                    ],
                )
                rows = cur.fetchall()
            conn.rollback()
        return {
            int(row["user_playbook_id"]): PlaybookAggregationClusterMatch(
                str(row["cluster_id"]),
                float(row["similarity"]),
                int(row["agent_playbook_id"]),
            )
            for row in rows
            if row["cluster_id"] is not None
        }

    def create_playbook_aggregation_cluster(
        self,
        *,
        cluster_id: str,
        agent_version: str,
        agent_playbook_id: int | None,
        centroid_embedding: list[float],
        member_count: int,
        embedding_model: str,
    ) -> None:
        if not centroid_embedding:
            raise ValueError("cluster centroid embedding must be non-empty")
        if member_count <= 0:
            raise ValueError("cluster member_count must be positive")
        dimension = len(centroid_embedding)
        with self._writer_connection(operation="aggregation.create_cluster") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "INSERT INTO {} (cluster_id, agent_version, agent_playbook_id, "
                        "centroid, vector_sum, member_count, embedding_model, "
                        "embedding_dimension) VALUES (%s::uuid, %s, %s, %s::public.vector, "
                        "%s::public.vector, %s, %s, %s) ON CONFLICT (cluster_id) "
                        "DO UPDATE SET agent_version=excluded.agent_version, "
                        "agent_playbook_id=excluded.agent_playbook_id, "
                        "centroid=excluded.centroid, vector_sum=excluded.vector_sum, "
                        "member_count=excluded.member_count, "
                        "embedding_model=excluded.embedding_model, "
                        "embedding_dimension=excluded.embedding_dimension, "
                        "normalization=excluded.normalization, state='active', "
                        "dirty=false, rebuild_cursor=0"
                    ).format(self._aggregation_table("playbook_aggregation_cluster")),
                    [
                        cluster_id,
                        agent_version,
                        agent_playbook_id,
                        json.dumps(centroid_embedding),
                        None,
                        member_count,
                        embedding_model,
                        dimension,
                    ],
                )
            conn.commit()

    def attach_playbook_aggregation_items(
        self,
        *,
        agent_version: str,
        attachments: list[tuple[int, str]],
    ) -> None:
        if not attachments:
            return
        item_ids = [item_id for item_id, _cluster_id in attachments]
        if len(item_ids) != len(set(item_ids)):
            raise ValueError("aggregation attachment IDs must be unique")
        grouped: dict[str, list[int]] = {}
        for item_id, cluster_id in attachments:
            grouped.setdefault(cluster_id, []).append(item_id)
        cluster_payload = Json([{"cluster_id": value} for value in grouped])
        attachment_payload = Json(
            [
                {"user_playbook_id": item_id, "cluster_id": cluster_id}
                for item_id, cluster_id in attachments
            ]
        )
        with self._writer_connection(operation="aggregation.attach_cluster") as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        sql.SQL(
                            "WITH targets AS (SELECT * FROM jsonb_to_recordset(%s::jsonb) "
                            "AS value(cluster_id uuid)) SELECT c.cluster_id::text, "
                            "c.member_count "
                            "FROM {} c JOIN targets t ON t.cluster_id=c.cluster_id "
                            "WHERE c.agent_version=%s AND c.state='active' FOR UPDATE"
                        ).format(
                            self._aggregation_table("playbook_aggregation_cluster")
                        ),
                        [cluster_payload, agent_version],
                    )
                    cluster_rows = {
                        str(row["cluster_id"]): row for row in cur.fetchall()
                    }
                    if set(cluster_rows) != set(grouped):
                        raise RuntimeError(
                            "aggregation cluster is not active for agent version"
                        )
                    cluster_updates: list[dict[str, Any]] = []
                    for cluster_id, item_ids_for_cluster in grouped.items():
                        cluster = cluster_rows[cluster_id]
                        cluster_updates.append(
                            {
                                "cluster_id": cluster_id,
                                "member_count": int(cluster["member_count"])
                                + len(item_ids_for_cluster),
                            }
                        )
                    cur.execute(
                        sql.SQL(
                            "WITH attachments AS (SELECT * FROM "
                            "jsonb_to_recordset(%s::jsonb) AS value("
                            "user_playbook_id bigint, cluster_id uuid)) UPDATE {} i "
                            "SET disposition='cluster_member', cluster_id=a.cluster_id, "
                            "reason='centroid_match', updated_at=floor(extract(epoch "
                            "FROM clock_timestamp()))::bigint FROM attachments a "
                            "WHERE i.agent_version=%s AND i.user_playbook_id="
                            "a.user_playbook_id AND i.disposition='residual'"
                        ).format(self._aggregation_table("playbook_aggregation_item")),
                        [attachment_payload, agent_version],
                    )
                    if cur.rowcount != len(attachments):
                        raise RuntimeError(
                            "aggregation residual attachment lost its state"
                        )
                    cur.execute(
                        sql.SQL(
                            "WITH updates AS (SELECT * FROM jsonb_to_recordset(%s::jsonb) "
                            "AS value(cluster_id uuid, member_count integer)) "
                            "UPDATE {} c SET member_count=u.member_count FROM updates u WHERE "
                            "c.cluster_id=u.cluster_id AND c.agent_version=%s "
                            "AND c.state='active'"
                        ).format(
                            self._aggregation_table("playbook_aggregation_cluster")
                        ),
                        [Json(cluster_updates), agent_version],
                    )
                    if cur.rowcount != len(grouped):
                        raise RuntimeError("aggregation cluster is not active")
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def replace_playbook_aggregation_cluster_agent(
        self,
        *,
        cluster_id: str,
        agent_version: str,
        expected_agent_playbook_id: int,
        replacement_agent_playbook_id: int,
        centroid_embedding: list[float],
        embedding_model: str,
    ) -> None:
        if not centroid_embedding:
            raise ValueError("cluster centroid embedding must be non-empty")
        with self._writer_connection(
            operation="aggregation.replace_cluster_agent"
        ) as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL(
                            "UPDATE {} SET agent_playbook_id=%s, centroid=%s::public.vector, "
                            "vector_sum=NULL, embedding_model=%s, dirty=false WHERE "
                            "cluster_id=%s::uuid AND agent_version=%s AND state='active' "
                            "AND agent_playbook_id=%s AND embedding_dimension=%s"
                        ).format(
                            self._aggregation_table("playbook_aggregation_cluster")
                        ),
                        [
                            replacement_agent_playbook_id,
                            json.dumps(centroid_embedding),
                            embedding_model,
                            cluster_id,
                            agent_version,
                            expected_agent_playbook_id,
                            len(centroid_embedding),
                        ],
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError("aggregation cluster agent changed")
                conn.commit()
            except Exception:
                conn.rollback()
                raise
