"""Playbook retrieval log storage for native Postgres."""

from __future__ import annotations

import time
from typing import Any

from psycopg2 import sql
from psycopg2.extras import Json

from reflexio.models.api_schema.domain.entities import (
    PlaybookRetrievalLog,
    PlaybookRetrievalLogItem,
)
from reflexio.server.services.storage.postgres_storage._base import PostgresStorageBase

handle_exceptions = PostgresStorageBase.handle_exceptions


class PostgresRetrievalLogMixin:
    """Persist retrieval headers and ordered attribution snapshots in Postgres."""

    org_id: str
    pool: Any
    _fetch_all: Any
    _table_identifier: Any

    @handle_exceptions
    def save_playbook_retrieval_log(self, log: PlaybookRetrievalLog) -> int:
        created_at = log.created_at or int(time.time())
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {} (
                            org_id, request_id, session_id, interaction_id, user_id,
                            query, agent_version, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING retrieval_log_id
                        """
                    ).format(self._table_identifier("playbook_retrieval_logs")),
                    (
                        self.org_id,
                        log.request_id,
                        log.session_id,
                        log.interaction_id,
                        log.user_id,
                        log.query,
                        log.agent_version,
                        created_at,
                    ),
                )
                retrieval_log_id = int(cur.fetchone()[0])
                for item in log.shown_items:
                    cur.execute(
                        sql.SQL(
                            """
                            INSERT INTO {} (
                                retrieval_log_id, ordinal, agent_playbook_id,
                                source_user_playbook_ids,
                                source_interaction_ids_by_user_playbook_id
                            )
                            VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)
                            RETURNING retrieval_log_item_id
                            """
                        ).format(
                            self._table_identifier("playbook_retrieval_log_items")
                        ),
                        (
                            retrieval_log_id,
                            item.ordinal,
                            item.agent_playbook_id,
                            Json(item.source_user_playbook_ids),
                            Json(item.source_interaction_ids_by_user_playbook_id),
                        ),
                    )
                    item.retrieval_log_item_id = int(cur.fetchone()[0])
                    item.retrieval_log_id = retrieval_log_id
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)
        log.retrieval_log_id = retrieval_log_id
        log.created_at = created_at
        return retrieval_log_id

    @handle_exceptions
    def get_playbook_retrieval_logs(
        self,
        *,
        session_id: str | None = None,
        request_id: str | None = None,
        interaction_id: int | None = None,
        user_id: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[PlaybookRetrievalLog]:
        clauses: list[sql.Composable] = [sql.SQL("org_id = %s")]
        params: list[Any] = [self.org_id]
        for column, value in (
            ("session_id", session_id),
            ("request_id", request_id),
            ("interaction_id", interaction_id),
            ("user_id", user_id),
        ):
            if value is not None:
                clauses.append(sql.SQL("{} = %s").format(sql.Identifier(column)))
                params.append(value)
        if start_time is not None:
            clauses.append(sql.SQL("created_at >= %s"))
            params.append(start_time)
        if end_time is not None:
            clauses.append(sql.SQL("created_at <= %s"))
            params.append(end_time)

        headers = self._fetch_all(
            sql.SQL("SELECT * FROM {} WHERE {} ORDER BY created_at, retrieval_log_id").format(
                self._table_identifier("playbook_retrieval_logs"),
                sql.SQL(" AND ").join(clauses),
            ),
            params,
        )
        if not headers:
            return []
        ids = [int(row["retrieval_log_id"]) for row in headers]
        item_rows = self._fetch_all(
            sql.SQL(
                "SELECT * FROM {} WHERE retrieval_log_id = ANY(%s) "
                "ORDER BY retrieval_log_id, ordinal"
            ).format(self._table_identifier("playbook_retrieval_log_items")),
            [ids],
        )
        items_by_log: dict[int, list[PlaybookRetrievalLogItem]] = {}
        for row in item_rows:
            rid = int(row["retrieval_log_id"])
            items_by_log.setdefault(rid, []).append(
                PlaybookRetrievalLogItem(
                    retrieval_log_item_id=int(row["retrieval_log_item_id"]),
                    retrieval_log_id=rid,
                    ordinal=int(row["ordinal"]),
                    agent_playbook_id=int(row["agent_playbook_id"]),
                    source_user_playbook_ids=[
                        int(value) for value in (row.get("source_user_playbook_ids") or [])
                    ],
                    source_interaction_ids_by_user_playbook_id={
                        str(key): [int(v) for v in value]
                        for key, value in (
                            row.get("source_interaction_ids_by_user_playbook_id") or {}
                        ).items()
                    },
                )
            )
        return [
            PlaybookRetrievalLog(
                retrieval_log_id=int(row["retrieval_log_id"]),
                request_id=str(row["request_id"]),
                session_id=str(row["session_id"]),
                interaction_id=row.get("interaction_id"),
                user_id=str(row["user_id"]),
                query=row.get("query"),
                agent_version=row.get("agent_version"),
                shown_items=items_by_log.get(int(row["retrieval_log_id"]), []),
                created_at=int(row["created_at"]),
            )
            for row in headers
        ]
