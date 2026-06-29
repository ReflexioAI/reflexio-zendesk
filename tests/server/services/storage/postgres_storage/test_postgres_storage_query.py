"""Tests for the native Postgres query facade."""

from reflexio.server.services.storage.postgres_storage._query import (
    PostgresQuery,
    _parse_or_expression,
    _prepare_value,
)


class _StorageWithColumns:
    def __init__(self, columns: set[str]) -> None:
        self.columns = columns

    def _table_columns(self, _table: str) -> set[str]:
        return self.columns


def test_parse_or_expression_supports_status_eq() -> None:
    params: list[object] = []

    _parse_or_expression("status.is.null,status.eq.archived", params)

    assert params == ["archived"]


def test_parse_or_expression_preserves_in_list_commas() -> None:
    params: list[object] = []

    _parse_or_expression("status.is.null,status.in.(archived,current)", params)

    assert params == [["archived", "current"]]


def test_playbook_aggregation_columns_map_to_migration_names() -> None:
    query = PostgresQuery(
        storage=_StorageWithColumns(
            {"added_feedbacks", "removed_feedbacks", "updated_feedbacks"}
        ),
        table="playbook_aggregation_change_logs",
    )

    assert query._physical_column("added_playbooks") == "added_feedbacks"
    assert query._physical_column("removed_playbooks") == "removed_feedbacks"
    assert query._physical_column("updated_playbooks") == "updated_feedbacks"
    assert query._normalize_row({"added_feedbacks": [{"content": "new"}]}) == {
        "added_feedbacks": [{"content": "new"}],
        "added_playbooks": [{"content": "new"}],
    }


def test_playbook_aggregation_columns_keep_current_names_when_present() -> None:
    query = PostgresQuery(
        storage=_StorageWithColumns(
            {"added_playbooks", "removed_playbooks", "updated_playbooks"}
        ),
        table="playbook_aggregation_change_logs",
    )

    assert query._physical_column("added_playbooks") == "added_playbooks"
    assert query._physical_column("removed_playbooks") == "removed_playbooks"
    assert query._physical_column("updated_playbooks") == "updated_playbooks"


def test_text_id_range_filter_converts_delete_sentinel() -> None:
    query = PostgresQuery(storage=object(), table="profiles").gte("profile_id", 0)
    params: list[object] = []

    query._where_sql(params)

    assert params == ["0"]


def test_profile_source_interaction_ids_prepare_as_jsonb() -> None:
    value = _prepare_value("source_interaction_ids", [1, 2], table="profiles")

    assert value.adapted == [1, 2]


def test_user_playbook_source_interaction_ids_prepare_as_array() -> None:
    value = _prepare_value("source_interaction_ids", [1, 2], table="user_playbooks")

    assert value == [1, 2]
