"""Typing helpers shared by Supabase storage mixins."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from psycopg2 import sql


class SchemaScopedClient:
    """Helpers supplied by PostgresStorageBase through the concrete MRO."""

    if TYPE_CHECKING:

        def _table(self, name: str) -> Any: ...

        def _rpc(self, name: str, params: dict[str, Any]) -> Any: ...

        def _fetch_all(
            self, query: sql.Composable, params: list[Any] | None = None
        ) -> list[dict[str, Any]]: ...

        def _table_identifier(self, name: str) -> sql.Composable: ...

        def _delete_all_text_keyed(self, table: str, key_column: str) -> None: ...
