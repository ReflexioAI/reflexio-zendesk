"""Lineage storage for native Postgres."""

from __future__ import annotations

import time
import uuid
from typing import Any, Literal, cast

from psycopg2 import sql
from psycopg2.extras import Json

from reflexio.models.api_schema.domain.entities import LineageContext, LineageEvent
from reflexio.models.api_schema.domain.enums import Status
from reflexio.server.services.storage.postgres_storage._base import PostgresStorageBase
from reflexio.server.tracing import capture_anomaly

EntityType = Literal["user_playbook", "agent_playbook", "profile"]

_EMPTY_REQUEST_ID_MSG = "request_id must be non-empty"
_GC_ELIGIBLE_STATUSES: frozenset[str] = frozenset(
    {Status.MERGED.value, Status.SUPERSEDED.value, Status.ARCHIVED.value}
)
_TABLE: dict[str, tuple[str, str]] = {
    "user_playbook": ("user_playbooks", "user_playbook_id"),
    "agent_playbook": ("agent_playbooks", "agent_playbook_id"),
    "profile": ("profiles", "profile_id"),
}

handle_exceptions = PostgresStorageBase.handle_exceptions


def _resolve_table(entity_type: str) -> tuple[str, str]:
    table = _TABLE.get(entity_type)
    if table is None:
        raise ValueError(f"unknown entity_type: {entity_type!r}")
    return table


class PostgresLineageMixin:
    """Postgres implementation of lineage/tombstone storage primitives."""

    org_id: str
    schema_name: str
    pool: Any
    _opensearch: Any
    _fetch_all: Any
    _table_identifier: Any

    @handle_exceptions
    def append_lineage_event(self, event: LineageEvent) -> int:
        created = event.created_at or int(time.time())
        query = sql.SQL(
            """
            INSERT INTO {} (
                org_id, entity_type, entity_id, op, prov_relation, source_ids,
                actor, request_id, reason, created_at, from_status, to_status,
                status_namespace
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (org_id, entity_type, entity_id, op, request_id)
            DO UPDATE SET event_id = lineage_event.event_id
            RETURNING event_id
            """
        ).format(self._table_identifier("lineage_event"))
        rows = self._fetch_all(
            query,
            [
                event.org_id,
                event.entity_type,
                event.entity_id,
                event.op,
                event.prov_relation,
                Json(event.source_ids),
                event.actor,
                event.request_id,
                event.reason,
                created,
                event.from_status,
                event.to_status,
                event.status_namespace,
            ],
        )
        return int(rows[0]["event_id"]) if rows else 0

    @handle_exceptions
    def get_lineage_events(
        self,
        *,
        entity_type: str | None = None,
        entity_id: str | None = None,
        org_id: str | None = None,
        request_id: str | None = None,
    ) -> list[LineageEvent]:
        clauses: list[sql.Composable] = []
        params: list[Any] = []
        for column, value in (
            ("entity_type", entity_type),
            ("entity_id", entity_id),
            ("org_id", org_id),
            ("request_id", request_id),
        ):
            if value is not None:
                clauses.append(sql.SQL("{} = %s").format(sql.Identifier(column)))
                params.append(value)
        where = (
            sql.SQL(" WHERE ") + sql.SQL(" AND ").join(clauses)
            if clauses
            else sql.SQL("")
        )
        rows = self._fetch_all(
            sql.SQL("SELECT * FROM {}{} ORDER BY event_id").format(
                self._table_identifier("lineage_event"),
                where,
            ),
            params,
        )
        return [
            LineageEvent(
                event_id=int(row["event_id"]),
                org_id=str(row["org_id"]),
                entity_type=str(row["entity_type"]),
                entity_id=str(row["entity_id"]),
                op=str(row["op"]),
                prov_relation=str(row.get("prov_relation") or ""),
                source_ids=list(row.get("source_ids") or []),
                actor=str(row.get("actor") or ""),
                request_id=str(row.get("request_id") or ""),
                reason=str(row.get("reason") or ""),
                created_at=int(row["created_at"]),
                from_status=cast(str | None, row.get("from_status")),
                to_status=cast(str | None, row.get("to_status")),
                status_namespace=cast(str | None, row.get("status_namespace")),
            )
            for row in rows
        ]

    @handle_exceptions
    def merge_records(
        self,
        *,
        entity_type: EntityType,
        survivor_id: str,
        source_ids: list[str],
        context: LineageContext,
    ) -> None:
        if not (context.request_id and context.request_id.strip()):
            raise ValueError(f"lineage merge: {_EMPTY_REQUEST_ID_MSG}")
        if not source_ids:
            return
        table, pk = _resolve_table(entity_type)
        now = int(time.time())
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "UPDATE {} SET status = %s, merged_into = %s, retired_at = %s "
                        "WHERE {} = ANY(%s) AND (status IS NULL OR status NOT IN %s)"
                    ).format(self._table_identifier(table), sql.Identifier(pk)),
                    (
                        Status.MERGED.value,
                        survivor_id,
                        now,
                        source_ids,
                        tuple(_GC_ELIGIBLE_STATUSES),
                    ),
                )
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {} (
                            org_id, entity_type, entity_id, op, prov_relation,
                            source_ids, actor, request_id, reason, created_at
                        )
                        VALUES (%s, %s, %s, 'merge', 'wasDerivedFrom', %s::jsonb, %s, %s, %s, %s)
                        ON CONFLICT (org_id, entity_type, entity_id, op, request_id) DO NOTHING
                        """
                    ).format(self._table_identifier("lineage_event")),
                    (
                        self.org_id,
                        entity_type,
                        survivor_id,
                        Json(source_ids),
                        context.actor,
                        context.request_id,
                        context.reason,
                        now,
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
        if self._opensearch:
            self._opensearch.delete_ids(table, source_ids)

    @handle_exceptions
    def supersede_record(
        self,
        *,
        entity_type: EntityType,
        incumbent_id: str,
        successor_id: str,
        context: LineageContext,
    ) -> bool:
        if not (context.request_id and context.request_id.strip()):
            raise ValueError(f"lineage supersede: {_EMPTY_REQUEST_ID_MSG}")
        table, pk = _resolve_table(entity_type)
        now = int(time.time())
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "UPDATE {} SET status = %s, superseded_by = %s, retired_at = %s "
                        "WHERE {} = %s AND status IS NULL"
                    ).format(self._table_identifier(table), sql.Identifier(pk)),
                    (Status.SUPERSEDED.value, successor_id, now, incumbent_id),
                )
                changed = cur.rowcount > 0
                if changed:
                    cur.execute(
                        sql.SQL(
                            """
                            INSERT INTO {} (
                                org_id, entity_type, entity_id, op, prov_relation,
                                source_ids, actor, request_id, reason, created_at
                            )
                            VALUES (%s, %s, %s, 'revise', 'wasRevisionOf', %s::jsonb, %s, %s, %s, %s)
                            ON CONFLICT (org_id, entity_type, entity_id, op, request_id) DO NOTHING
                            """
                        ).format(self._table_identifier("lineage_event")),
                        (
                            self.org_id,
                            entity_type,
                            successor_id,
                            Json([incumbent_id]),
                            context.actor,
                            context.request_id,
                            context.reason,
                            now,
                        ),
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
        if changed and self._opensearch:
            self._opensearch.delete_ids(table, [incumbent_id])
        return changed

    @handle_exceptions
    def has_inbound_lineage_refs(
        self, *, entity_type: EntityType, entity_id: str
    ) -> bool:
        table, _pk = _resolve_table(entity_type)
        rows = self._fetch_all(
            sql.SQL(
                "SELECT 1 FROM {} WHERE merged_into = %s OR superseded_by = %s LIMIT 1"
            ).format(self._table_identifier(table)),
            [entity_id, entity_id],
        )
        return bool(rows)

    @handle_exceptions
    def purge_content(self, *, entity_type: EntityType, entity_id: str) -> bool:
        table, pk = _resolve_table(entity_type)
        if entity_type == "agent_playbook":
            raise ValueError("purge_content: unsupported entity_type 'agent_playbook'")
        if entity_type == "profile":
            set_sql = sql.SQL(
                """
                content = '', user_id = '', generated_from_request_id = '', source = '',
                embedding = NULL, extractor_names = NULL, expanded_terms = NULL,
                tags = NULL, custom_features = NULL, notes = NULL, source_span = NULL,
                reader_angle = NULL, source_interaction_ids = '[]'::jsonb
                """
            )
        else:
            set_sql = sql.SQL(
                """
                content = '', user_id = NULL, request_id = '', source = NULL,
                "trigger" = NULL, rationale = NULL, blocking_issue = NULL,
                source_interaction_ids = NULL, embedding = NULL, expanded_terms = NULL,
                tags = NULL, source_span = NULL, notes = NULL, reader_angle = NULL
                """
            )
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("UPDATE {} SET {} WHERE {} = %s").format(
                        self._table_identifier(table), set_sql, sql.Identifier(pk)
                    ),
                    (entity_id,),
                )
                found = cur.rowcount > 0
                if found:
                    cur.execute(
                        sql.SQL(
                            """
                            INSERT INTO {} (
                                org_id, entity_type, entity_id, op, prov_relation,
                                source_ids, actor, request_id, reason, created_at
                            )
                            VALUES (%s, %s, %s, 'purge', 'wasPurged', '[]'::jsonb, 'erasure', %s, 'content_purge', %s)
                            ON CONFLICT (org_id, entity_type, entity_id, op, request_id) DO NOTHING
                            """
                        ).format(self._table_identifier("lineage_event")),
                        (
                            self.org_id,
                            entity_type,
                            entity_id,
                            f"purge_{entity_id}",
                            int(time.time()),
                        ),
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
        if found and self._opensearch:
            self._opensearch.delete_ids(table, [entity_id])
        return found

    def _is_on_legal_hold(
        self,
        org_id: str,  # noqa: ARG002
        entity_type: str,  # noqa: ARG002
        entity_id: str,  # noqa: ARG002
    ) -> bool:
        return False

    @handle_exceptions
    def list_org_ids(self) -> list[str]:
        rows = self._fetch_all(
            sql.SQL(
                """
                SELECT org_id FROM lineage_event
                UNION SELECT %s AS org_id
                ORDER BY org_id
                """
            ),
            [self.org_id],
        )
        return [str(row["org_id"]) for row in rows]

    @handle_exceptions
    def gc_expired_tombstones(
        self, *, entity_type: str, older_than_epoch: int, limit: int = 1000
    ) -> int:
        if limit <= 0:
            return 0
        table, pk = _resolve_table(entity_type)
        rows = self._fetch_all(
            sql.SQL(
                "SELECT {} FROM {} WHERE status = ANY(%s) "
                "AND retired_at IS NOT NULL AND retired_at < %s "
                "ORDER BY retired_at ASC"
            ).format(sql.Identifier(pk), self._table_identifier(table)),
            [list(_GC_ELIGIBLE_STATUSES), older_than_epoch],
        )
        ids_to_delete: list[str] = []
        for row in rows:
            entity_id = str(row[pk])
            if self._is_on_legal_hold(self.org_id, entity_type, entity_id):
                capture_anomaly(
                    "lineage.gc.legal_hold_skip",
                    level="info",
                    org_id=self.org_id,
                    entity_type=entity_type,
                    entity_id=entity_id,
                )
                continue
            ids_to_delete.append(entity_id)
            if len(ids_to_delete) >= limit:
                break
        if not ids_to_delete:
            return 0

        batch_request_id = uuid.uuid4().hex
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                for entity_id in ids_to_delete:
                    cur.execute(
                        sql.SQL(
                            """
                            INSERT INTO {} (
                                org_id, entity_type, entity_id, op, prov_relation,
                                source_ids, actor, request_id, reason, created_at
                            )
                            VALUES (%s, %s, %s, 'hard_delete', 'wasInvalidatedBy', '[]'::jsonb, 'system', %s, 'ttl-gc', %s)
                            ON CONFLICT (org_id, entity_type, entity_id, op, request_id) DO NOTHING
                            """
                        ).format(self._table_identifier("lineage_event")),
                        (
                            self.org_id,
                            entity_type,
                            entity_id,
                            batch_request_id,
                            int(time.time()),
                        ),
                    )
                cur.execute(
                    sql.SQL("DELETE FROM {} WHERE {} = ANY(%s)").format(
                        self._table_identifier(table), sql.Identifier(pk)
                    ),
                    (ids_to_delete,),
                )
                deleted = cur.rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
        if self._opensearch:
            self._opensearch.delete_ids(table, ids_to_delete)
        return int(deleted)
