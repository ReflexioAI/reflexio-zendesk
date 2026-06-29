"""Small PostgREST-like SQL facade for native Postgres storage."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, RealDictCursor

_JSON_COLUMNS = {
    "added_feedbacks",
    "added_playbooks",
    "added_profiles",
    "allowed_tools",
    "blocking_issue",
    "blocking_issues",
    "citations",
    "custom_features",
    "extractor_names",
    "mentioned_profiles",
    "operation_state",
    "playbook_metadata_json",
    "project_ids",
    "removed_feedbacks",
    "removed_playbooks",
    "removed_profiles",
    "structured_data",
    "tools_used",
    "updated_feedbacks",
    "updated_playbooks",
    "user_playbook_ids",
}
_ARRAY_COLUMNS = {"parent_candidate_ids", "source_interaction_ids"}
_TEXT_COLUMNS = {
    "profiles": {"profile_id"},
    "requests": {"request_id"},
}
_PRIMARY_KEYS = {
    "_operation_state": "service_name",
    "agent_playbooks": "agent_playbook_id",
    "agent_success_evaluation_result": "result_id",
    "interactions": "interaction_id",
    "playbook_aggregation_change_logs": "id",
    "profile_change_logs": "id",
    "profiles": "profile_id",
    "requests": "request_id",
    "share_links": "id",
    "skills": "skill_id",
    "user_playbooks": "user_playbook_id",
}
_COLUMN_ALIASES = {
    "playbook_aggregation_change_logs": {
        "added_playbooks": "added_feedbacks",
        "removed_playbooks": "removed_feedbacks",
        "updated_playbooks": "updated_feedbacks",
    }
}


@dataclass
class PostgresResponse:
    data: Any
    count: int | None = None


class PostgresQuery:
    def __init__(self, storage: Any, table: str) -> None:
        self.storage = storage
        self.table = table
        self._operation: Literal["select", "insert", "upsert", "update", "delete"] = (
            "select"
        )
        self._select_expr = "*"
        self._count_exact = False
        self._filters: list[tuple[str, str, Any]] = []
        self._or_filters: list[str] = []
        self._order_by: tuple[str, bool] | None = None
        self._limit: int | None = None
        self._offset: int | None = None
        self._payload: Any = None

    def select(self, columns: str = "*", count: str | None = None) -> PostgresQuery:
        self._operation = "select"
        self._select_expr = columns
        self._count_exact = count == "exact"
        return self

    def insert(self, data: Any) -> PostgresQuery:
        self._operation = "insert"
        self._payload = data
        return self

    def upsert(self, data: Any) -> PostgresQuery:
        self._operation = "upsert"
        self._payload = data
        return self

    def update(self, data: dict[str, Any]) -> PostgresQuery:
        self._operation = "update"
        self._payload = data
        return self

    def delete(self) -> PostgresQuery:
        self._operation = "delete"
        return self

    def eq(self, column: str, value: Any) -> PostgresQuery:
        self._filters.append((column, "=", value))
        return self

    def neq(self, column: str, value: Any) -> PostgresQuery:
        self._filters.append((column, "<>", value))
        return self

    def gt(self, column: str, value: Any) -> PostgresQuery:
        self._filters.append((column, ">", value))
        return self

    def gte(self, column: str, value: Any) -> PostgresQuery:
        self._filters.append((column, ">=", value))
        return self

    def lt(self, column: str, value: Any) -> PostgresQuery:
        self._filters.append((column, "<", value))
        return self

    def lte(self, column: str, value: Any) -> PostgresQuery:
        self._filters.append((column, "<=", value))
        return self

    def in_(self, column: str, values: list[Any] | tuple[Any, ...]) -> PostgresQuery:
        self._filters.append((column, "IN", list(values)))
        return self

    def contains(self, column: str, value: Any) -> PostgresQuery:
        self._filters.append((column, "@>", value))
        return self

    def is_(self, column: str, value: str) -> PostgresQuery:
        if value != "null":
            raise ValueError(f"Unsupported is_ value: {value}")
        self._filters.append((column, "IS NULL", None))
        return self

    def or_(self, expression: str) -> PostgresQuery:
        self._or_filters.append(expression)
        return self

    def order(self, column: str, desc: bool = False) -> PostgresQuery:
        self._order_by = (column, desc)
        return self

    def limit(self, value: int) -> PostgresQuery:
        self._limit = value
        return self

    def offset(self, value: int) -> PostgresQuery:
        self._offset = value
        return self

    def range(self, start: int, end: int) -> PostgresQuery:
        self._offset = start
        self._limit = end - start + 1
        return self

    def execute(self) -> PostgresResponse:
        if self._operation == "select":
            return self._execute_select()
        if self._operation in {"insert", "upsert"}:
            return self._execute_insert(upsert=self._operation == "upsert")
        if self._operation == "update":
            return self._execute_update()
        if self._operation == "delete":
            return self._execute_delete()
        raise ValueError(f"Unsupported operation: {self._operation}")

    def _execute_select(self) -> PostgresResponse:
        count = self._execute_count() if self._count_exact else None
        join_interactions = (
            self.table == "requests" and "interactions(" in self._select_expr
        )
        select_sql = (
            sql.SQL("*")
            if join_interactions
            else _select_sql(self._select_expr, self._column_aliases())
        )
        query = sql.SQL("SELECT {} FROM {}").format(select_sql, self._table_sql())
        params: list[Any] = []
        where_sql = self._where_sql(params)
        if where_sql is not None:
            query += sql.SQL(" WHERE ") + where_sql
        if self._order_by:
            column, desc = self._order_by
            query += sql.SQL(" ORDER BY {} {}").format(
                sql.Identifier(column),
                sql.SQL("DESC" if desc else "ASC"),
            )
        if self._limit is not None:
            query += sql.SQL(" LIMIT %s")
            params.append(self._limit)
        if self._offset is not None:
            query += sql.SQL(" OFFSET %s")
            params.append(self._offset)

        rows = [
            self._normalize_row(row) for row in self.storage._fetch_all(query, params)
        ]
        if join_interactions and rows:
            request_ids = [row["request_id"] for row in rows]
            interaction_rows = self.storage._fetch_all(
                sql.SQL("SELECT {} FROM {} WHERE {} = ANY(%s) ORDER BY {} ASC").format(
                    _select_sql(self.storage._interaction_columns),
                    self.storage._table_identifier("interactions"),
                    sql.Identifier("request_id"),
                    sql.Identifier("created_at"),
                ),
                [request_ids],
            )
            by_request: dict[str, list[dict[str, Any]]] = {}
            for interaction in interaction_rows:
                by_request.setdefault(interaction["request_id"], []).append(interaction)
            for row in rows:
                row["interactions"] = by_request.get(row["request_id"], [])
        return PostgresResponse(data=rows, count=count)

    def _execute_count(self) -> int:
        query = sql.SQL("SELECT count(*) AS count FROM {}").format(self._table_sql())
        params: list[Any] = []
        where_sql = self._where_sql(params)
        if where_sql is not None:
            query += sql.SQL(" WHERE ") + where_sql
        rows = self.storage._fetch_all(query, params)
        return int(rows[0]["count"]) if rows else 0

    def _execute_insert(self, *, upsert: bool) -> PostgresResponse:
        rows = self._payload if isinstance(self._payload, list) else [self._payload]
        if not rows:
            return PostgresResponse(data=[])
        returned: list[dict[str, Any]] = []
        for row in rows:
            cleaned = {
                k: v for k, v in row.items() if v is not None or k != "embedding"
            }
            columns = list(cleaned)
            values = [_prepare_value(k, cleaned[k]) for k in columns]
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                self._table_sql(),
                sql.SQL(", ").join(
                    sql.Identifier(self._physical_column(c)) for c in columns
                ),
                sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            )
            pk = _PRIMARY_KEYS.get(self.table)
            if upsert and pk and pk in cleaned:
                assignments = [
                    sql.SQL("{} = EXCLUDED.{}").format(
                        sql.Identifier(self._physical_column(c)),
                        sql.Identifier(self._physical_column(c)),
                    )
                    for c in columns
                    if c != pk
                ]
                if assignments:
                    query += sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {}").format(
                        sql.Identifier(pk),
                        sql.SQL(", ").join(assignments),
                    )
                else:
                    query += sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(
                        sql.Identifier(pk)
                    )
            query += sql.SQL(" RETURNING *")
            returned.extend(
                self._normalize_row(row)
                for row in self.storage._fetch_all(query, values)
            )
        return PostgresResponse(data=returned)

    def _execute_update(self) -> PostgresResponse:
        payload = self._payload or {}
        columns = list(payload)
        params = [_prepare_value(k, payload[k]) for k in columns]
        query = sql.SQL("UPDATE {} SET {}").format(
            self._table_sql(),
            sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(self._physical_column(c)))
                for c in columns
            ),
        )
        where_sql = self._where_sql(params)
        if where_sql is not None:
            query += sql.SQL(" WHERE ") + where_sql
        query += sql.SQL(" RETURNING *")
        return PostgresResponse(
            data=[
                self._normalize_row(row)
                for row in self.storage._fetch_all(query, params)
            ]
        )

    def _execute_delete(self) -> PostgresResponse:
        params: list[Any] = []
        query = sql.SQL("DELETE FROM {}").format(self._table_sql())
        where_sql = self._where_sql(params)
        if where_sql is not None:
            query += sql.SQL(" WHERE ") + where_sql
        query += sql.SQL(" RETURNING *")
        return PostgresResponse(
            data=[
                self._normalize_row(row)
                for row in self.storage._fetch_all(query, params)
            ]
        )

    def _where_sql(self, params: list[Any]) -> sql.Composable | None:
        clauses: list[sql.Composable] = []
        for column, op, value in self._filters:
            ident = sql.Identifier(self._physical_column(column))
            if op == "IS NULL":
                clauses.append(sql.SQL("{} IS NULL").format(ident))
            elif op == "IN":
                clauses.append(sql.SQL("{} = ANY(%s)").format(ident))
                params.append(value)
            elif op == "@>":
                clauses.append(sql.SQL("{} @> %s::jsonb").format(ident))
                params.append(Json(value))
            else:
                clauses.append(sql.SQL("{} {} %s").format(ident, sql.SQL(op)))
                params.append(self._filter_value(column, op, value))
        for expression in self._or_filters:
            clauses.append(_parse_or_expression(expression, params))  # noqa: PERF401
        if not clauses:
            return None
        return sql.SQL(" AND ").join(clauses)

    def _table_sql(self) -> sql.Composable:
        return self.storage._table_identifier(self.table)

    def _column_aliases(self) -> dict[str, str]:
        return _COLUMN_ALIASES.get(self.table, {})

    def _physical_column(self, column: str) -> str:
        aliased = self._column_aliases().get(column)
        if not aliased:
            return column
        table_columns = getattr(self.storage, "_table_columns", lambda _table: set())(
            self.table
        )
        if aliased in table_columns:
            return aliased
        return column

    def _filter_value(self, column: str, op: str, value: Any) -> Any:
        if (
            op in {">", ">=", "<", "<="}
            and column in _TEXT_COLUMNS.get(self.table, set())
            and isinstance(value, int)
        ):
            return str(value)
        return value

    def _normalize_row(self, row: dict[str, Any]) -> dict[str, Any]:
        aliases = self._column_aliases()
        if not aliases:
            return row
        normalized = dict(row)
        for logical, physical in aliases.items():
            if physical in normalized and logical not in normalized:
                normalized[logical] = normalized[physical]
        return normalized


class PostgresRpc:
    def __init__(self, storage: Any, function_name: str, params: dict[str, Any] | None):
        self.storage = storage
        self.function_name = function_name
        self.params = params or {}

    def execute(self) -> PostgresResponse:
        fragments: list[sql.Composable] = []
        values: list[Any] = []
        for name, value in self.params.items():
            placeholder = (
                sql.SQL("%s::public.vector")
                if name.endswith("embedding")
                else sql.SQL("%s")
            )
            fragments.append(
                sql.SQL("{} => {}").format(sql.Identifier(name), placeholder)
            )
            values.append(_prepare_rpc_value(name, value))
        query = sql.SQL("SELECT * FROM {}({})").format(
            self.storage._function_identifier(self.function_name),
            sql.SQL(", ").join(fragments),
        )
        rows = self.storage._fetch_all(query, values)
        if len(rows) == 1 and set(rows[0]) == {self.function_name}:
            return PostgresResponse(data=rows[0][self.function_name])
        return PostgresResponse(data=rows)


def _prepare_value(column: str, value: Any) -> Any:
    if column == "embedding" and isinstance(value, list):
        return _vector_literal(value)
    if column in _JSON_COLUMNS and isinstance(value, (dict, list)):
        return Json(value)
    if isinstance(value, (dict,)) or (
        isinstance(value, list) and column not in _ARRAY_COLUMNS
    ):
        return Json(value)
    return value


def _prepare_rpc_value(name: str, value: Any) -> Any:
    if name.endswith("embedding") and isinstance(value, list):
        return _vector_literal(value)
    if isinstance(value, dict):
        return Json(value)
    return value


def _vector_literal(value: list[Any]) -> str:
    return "[" + ",".join(str(float(v)) for v in value) + "]"


def _select_sql(
    select_expr: str, column_aliases: dict[str, str] | None = None
) -> sql.Composable:
    if select_expr.strip() == "*":
        return sql.SQL("*")
    aliases = column_aliases or {}
    columns = [
        aliases.get(_normalize_column(c), _normalize_column(c))
        for c in _split_select_columns(select_expr)
    ]
    return sql.SQL(", ").join(sql.Identifier(c) for c in columns)


def _split_select_columns(select_expr: str) -> list[str]:
    return _split_top_level_csv(select_expr)


def _split_top_level_csv(value: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    in_quote = False
    for char in value:
        if char == '"':
            in_quote = not in_quote
        elif not in_quote and char == "(":
            depth += 1
        elif not in_quote and char == ")":
            depth -= 1
        if char == "," and depth == 0 and not in_quote:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if current:
        parts.append("".join(current).strip())
    return parts


def _normalize_column(column: str) -> str:
    column = column.strip()
    if column.startswith('"') and column.endswith('"'):
        return column[1:-1]
    return column


def _parse_or_expression(expression: str, params: list[Any]) -> sql.Composable:
    pieces = [p.strip() for p in _split_top_level_csv(expression) if p.strip()]
    clauses: list[sql.Composable] = []
    for piece in pieces:
        null_match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\.is\.null", piece)
        if null_match:
            clauses.append(
                sql.SQL("{} IS NULL").format(sql.Identifier(null_match.group(1)))
            )
            continue
        eq_match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\.eq\.(.*)", piece)
        if eq_match:
            clauses.append(sql.SQL("{} = %s").format(sql.Identifier(eq_match.group(1))))
            params.append(eq_match.group(2))
            continue
        in_match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\.in\.\((.*)\)", piece)
        if in_match:
            values = [v for v in in_match.group(2).split(",") if v]
            clauses.append(
                sql.SQL("{} = ANY(%s)").format(sql.Identifier(in_match.group(1)))
            )
            params.append(values)
            continue
        raise ValueError(f"Unsupported OR expression: {piece}")
    if not clauses:
        return sql.SQL("TRUE")
    return sql.SQL("(") + sql.SQL(" OR ").join(clauses) + sql.SQL(")")


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, (datetime, date)):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


def execute_fetch_all(
    conn: psycopg2.extensions.connection,
    query: sql.Composable,
    params: list[Any],
    schema: str,
) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            sql.SQL("SET LOCAL search_path TO {}, public, extensions").format(
                sql.Identifier(schema)
            )
        )
        cursor.execute(query, params)
        if cursor.description is None:
            return []
        return [normalize_row(dict(row)) for row in cursor.fetchall()]
