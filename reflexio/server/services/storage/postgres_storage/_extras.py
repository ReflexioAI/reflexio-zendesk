"""Analytics and change log methods for Supabase storage."""

import logging
from datetime import UTC, datetime
from typing import Any

from reflexio.models.api_schema.service_schemas import (
    Interaction,
    PlaybookAggregationChangeLog,
    ProfileChangeLog,
)
from reflexio.server.services.storage.postgres_storage._playbook_converters import (
    playbook_aggregation_change_log_to_data,
    response_list_to_playbook_aggregation_change_logs,
)
from reflexio.server.services.storage.postgres_storage._profile_converters import (
    profile_change_log_to_data,
    response_list_to_profile_change_logs,
    response_to_interaction,
)

from ._base import (
    _INTERACTION_COLUMNS,
    PostgresStorageBase,
    _calculate_success_rate,
    _rows,
    _timestamp_to_iso,
)
from ._protocols import SchemaScopedClient

logger = logging.getLogger(__name__)

handle_exceptions = PostgresStorageBase.handle_exceptions


class ExtrasMixin(SchemaScopedClient):
    # Type hints for instance attributes/methods provided by PostgresStorageBase via MRO
    client: Any
    org_id: str
    _parse_datetime_to_timestamp: Any
    search_mode: Any
    vector_weight: float
    fts_weight: float

    # ==============================
    # Interaction query methods
    # ==============================

    @handle_exceptions
    def get_interactions_by_request_ids(
        self, request_ids: list[str]
    ) -> list[Interaction]:
        """Fetch interactions by their request IDs.

        Args:
            request_ids (list[str]): List of request IDs to fetch interactions for

        Returns:
            list[Interaction]: List of matching interaction objects
        """
        if not request_ids:
            return []

        response = (
            self._table("interactions")
            .select(_INTERACTION_COLUMNS)
            .in_("request_id", request_ids)
            .order("created_at", desc=False)
            .execute()
        )
        return [response_to_interaction(item) for item in _rows(response)]

    @handle_exceptions
    def get_interactions_by_ids(self, interaction_ids: list[int]) -> list[Interaction]:
        if not interaction_ids:
            return []
        response = (
            self._table("interactions")
            .select(_INTERACTION_COLUMNS)
            .in_("interaction_id", interaction_ids)
            .order("created_at", desc=False)
            .execute()
        )
        return [response_to_interaction(item) for item in _rows(response)]

    # ==============================
    # Dashboard methods
    # ==============================

    def _count_in_period(
        self,
        table: str,
        id_col: str,
        start: int | str,
        end: int | str,
        use_timestamp: bool = False,
    ) -> int:
        """Count rows in a table within a time period.

        Args:
            table (str): Table name
            id_col (str): Column to select for counting
            start (int | str): Period start (Unix int for bigint cols, ISO string for datetime cols)
            end (int | str): Period end
            use_timestamp (bool): If True, use bigint timestamp column; if False, use ISO datetime

        Returns:
            int: Row count in the period
        """
        time_col = (
            id_col.replace("_id", "_timestamp") if use_timestamp else "created_at"
        )
        if use_timestamp:
            time_col = "last_modified_timestamp"
        response = (
            self._table(table)
            .select(id_col, count="exact")  # type: ignore[reportArgumentType]
            .gte(time_col, start)
            .lte(time_col, end)
            .execute()
        )
        return response.count if response.count is not None else 0

    def _count_in_period_lt(
        self,
        table: str,
        id_col: str,
        start: int | str,
        end: int | str,
        use_timestamp: bool = False,
    ) -> int:
        """Count rows in a table within a time period (exclusive end).

        Args:
            table (str): Table name
            id_col (str): Column to select for counting
            start (int | str): Period start
            end (int | str): Period end (exclusive)
            use_timestamp (bool): If True, use bigint timestamp column

        Returns:
            int: Row count in the period
        """
        time_col = "last_modified_timestamp" if use_timestamp else "created_at"
        response = (
            self._table(table)
            .select(id_col, count="exact")  # type: ignore[reportArgumentType]
            .gte(time_col, start)
            .lt(time_col, end)
            .execute()
        )
        return response.count if response.count is not None else 0

    def _get_time_series_points(
        self,
        table: str,
        time_col: str,
        start: int | str,
        end: int | str,
        extra_cols: str = "",
    ) -> list[dict[str, Any]]:
        """Fetch time series data points from a table.

        Args:
            table (str): Table name
            time_col (str): Time column name
            start (int | str): Period start
            end (int | str): Period end
            extra_cols (str): Additional columns to select (comma-separated)

        Returns:
            list[dict[str, Any]]: Raw data rows from the query
        """
        select_cols = f"{time_col}, {extra_cols}" if extra_cols else time_col
        response = (
            self._table(table)
            .select(select_cols)
            .gte(time_col, start)
            .lte(time_col, end)
            .order(time_col)
            .execute()
        )
        return _rows(response)

    @handle_exceptions
    def get_dashboard_stats(self, days_back: int = 30) -> dict:
        """
        Get comprehensive dashboard statistics including counts and time-series data.
        Returns raw ungrouped time-series data for frontend grouping.

        Args:
            days_back (int): Number of days to include in time series data

        Returns:
            dict: Dictionary containing current_period, previous_period, and raw time_series data
        """
        current_time = int(datetime.now(UTC).timestamp())
        seconds_in_period = days_back * 24 * 60 * 60
        current_period_start = current_time - seconds_in_period
        previous_period_start = current_period_start - seconds_in_period

        current_time_iso = _timestamp_to_iso(current_time)
        current_period_start_iso = _timestamp_to_iso(current_period_start)
        previous_period_start_iso = _timestamp_to_iso(previous_period_start)

        # Count queries for current and previous periods
        current_stats: dict[str, int | float] = {
            "total_interactions": self._count_in_period(
                "interactions",
                "interaction_id",
                current_period_start_iso,
                current_time_iso,
            ),
            "total_profiles": self._count_in_period(
                "profiles",
                "profile_id",
                current_period_start,
                current_time,
                use_timestamp=True,
            ),
            "total_playbooks": (
                self._count_in_period(
                    "user_playbooks",
                    "user_playbook_id",
                    current_period_start_iso,
                    current_time_iso,
                )
                + self._count_in_period(
                    "agent_playbooks",
                    "agent_playbook_id",
                    current_period_start_iso,
                    current_time_iso,
                )
            ),
        }

        previous_stats: dict[str, int | float] = {
            "total_interactions": self._count_in_period_lt(
                "interactions",
                "interaction_id",
                previous_period_start_iso,
                current_period_start_iso,
            ),
            "total_profiles": self._count_in_period_lt(
                "profiles",
                "profile_id",
                previous_period_start,
                current_period_start,
                use_timestamp=True,
            ),
            "total_playbooks": (
                self._count_in_period_lt(
                    "user_playbooks",
                    "user_playbook_id",
                    previous_period_start_iso,
                    current_period_start_iso,
                )
                + self._count_in_period_lt(
                    "agent_playbooks",
                    "agent_playbook_id",
                    previous_period_start_iso,
                    current_period_start_iso,
                )
            ),
        }

        # Evaluation success rates
        eval_current_response = (
            self._table("agent_success_evaluation_result")
            .select("is_success")
            .gte("created_at", current_period_start_iso)
            .lte("created_at", current_time_iso)
            .execute()
        )
        eval_current_data = _rows(eval_current_response)
        eval_previous_response = (
            self._table("agent_success_evaluation_result")
            .select("is_success")
            .gte("created_at", previous_period_start_iso)
            .lt("created_at", current_period_start_iso)
            .execute()
        )
        eval_previous_data = _rows(eval_previous_response)

        current_stats["success_rate"] = _calculate_success_rate(eval_current_data)
        previous_stats["success_rate"] = _calculate_success_rate(eval_previous_data)

        # Time series data
        interactions_ts = self._get_time_series_points(
            "interactions", "created_at", current_period_start_iso, current_time_iso
        )
        profiles_ts = self._get_time_series_points(
            "profiles", "last_modified_timestamp", current_period_start, current_time
        )
        playbooks_ts = self._get_time_series_points(
            "user_playbooks", "created_at", current_period_start_iso, current_time_iso
        )
        evaluations_ts = self._get_time_series_points(
            "agent_success_evaluation_result",
            "created_at",
            current_period_start_iso,
            current_time_iso,
            extra_cols="is_success",
        )

        return {
            "current_period": current_stats,
            "previous_period": previous_stats,
            "interactions_time_series": sorted(
                [
                    {
                        "timestamp": self._parse_datetime_to_timestamp(r["created_at"]),
                        "value": 1,
                    }
                    for r in interactions_ts
                ],
                key=lambda x: x["timestamp"],
            ),
            "profiles_time_series": sorted(
                [
                    {"timestamp": r["last_modified_timestamp"], "value": 1}
                    for r in profiles_ts
                ],
                key=lambda x: x["timestamp"],
            ),
            "playbooks_time_series": sorted(
                [
                    {
                        "timestamp": self._parse_datetime_to_timestamp(r["created_at"]),
                        "value": 1,
                    }
                    for r in playbooks_ts
                ],
                key=lambda x: x["timestamp"],
            ),
            "evaluations_time_series": sorted(
                [
                    {
                        "timestamp": self._parse_datetime_to_timestamp(r["created_at"]),
                        "value": 100 if r.get("is_success") else 0,
                    }
                    for r in evaluations_ts
                ],
                key=lambda x: x["timestamp"],
            ),
        }

    def _group_by_time_bucket(
        self, timestamps: list[int], period_start: int, granularity: str
    ) -> dict:
        """
        Group timestamps into buckets based on granularity.

        Args:
            timestamps (list[int]): List of Unix timestamps
            period_start (int): Start of the period
            granularity (str): Time grouping ('daily', 'weekly', 'monthly')

        Returns:
            dict: Dictionary mapping bucket timestamp to count
        """
        buckets = {}
        for timestamp in timestamps:
            ts_key = self._get_time_bucket(timestamp, period_start, granularity)
            buckets[ts_key] = buckets.get(ts_key, 0) + 1
        return buckets

    def _get_time_bucket(
        self, timestamp: int, _period_start: int, granularity: str
    ) -> int:
        """
        Get the time bucket key for a timestamp based on granularity.

        Args:
            timestamp (int): The timestamp to bucket
            period_start (int): Start of the period
            granularity (str): 'daily', 'weekly', or 'monthly'

        Returns:
            int: Bucket timestamp (start of day/week/month)
        """
        from datetime import (
            timedelta,
        )  # Keep local import for infrequently used function

        dt = datetime.fromtimestamp(timestamp, tz=UTC)

        if granularity == "daily":
            bucket_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        elif granularity == "weekly":
            # Start of week (Monday)
            days_since_monday = dt.weekday()
            bucket_dt = (dt - timedelta(days=days_since_monday)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        elif granularity == "monthly":
            bucket_dt = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            # Default to daily
            bucket_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)

        return int(bucket_dt.timestamp())

    # ==============================
    # Statistics methods
    # ==============================

    @handle_exceptions
    def get_profile_statistics(self) -> dict:
        """Get profile count statistics by status using efficient SQL queries.

        Returns:
            dict with keys: current_count, pending_count, archived_count, expiring_soon_count
        """
        current_timestamp = int(datetime.now(UTC).timestamp())
        expiring_soon_timestamp = current_timestamp + (7 * 24 * 60 * 60)  # 7 days

        # Get all profiles that are not expired
        response = (
            self._table("profiles")
            .select("status, expiration_timestamp")
            .gte("expiration_timestamp", current_timestamp)
            .execute()
        )

        stats = {
            "current_count": 0,
            "pending_count": 0,
            "archived_count": 0,
            "expiring_soon_count": 0,
        }

        # Count profiles by status
        for profile_data in _rows(response):
            status = profile_data.get("status")
            expiration_timestamp = profile_data.get("expiration_timestamp")

            # Count by status
            if status is None:
                stats["current_count"] += 1
            elif status == "pending":
                stats["pending_count"] += 1
            elif status == "archived":
                stats["archived_count"] += 1

            # Count expiring soon (current profiles only)
            if (
                status is None
                and expiration_timestamp is not None
                and expiration_timestamp <= expiring_soon_timestamp
            ):
                stats["expiring_soon_count"] += 1

        return stats

    # ==============================
    # Profile Change Log methods
    # ==============================

    @handle_exceptions
    def add_profile_change_log(self, profile_change_log: ProfileChangeLog) -> None:
        data = profile_change_log_to_data(profile_change_log)
        self._table("profile_change_logs").upsert(data).execute()

    @handle_exceptions
    def get_profile_change_logs(self, limit: int = 100) -> list[ProfileChangeLog]:
        response = (
            self._table("profile_change_logs")
            .select("*")
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        return response_list_to_profile_change_logs(_rows(response))

    @handle_exceptions
    def delete_profile_change_log_for_user(self, user_id: str) -> None:
        self._table("profile_change_logs").delete().eq("user_id", user_id).execute()

    @handle_exceptions
    def delete_all_profile_change_logs(self) -> None:
        self._table("profile_change_logs").delete().gte("id", 0).execute()

    # ==============================
    # Playbook Aggregation Change Log methods
    # ==============================

    @handle_exceptions
    def add_playbook_aggregation_change_log(
        self, change_log: PlaybookAggregationChangeLog
    ) -> None:
        data = playbook_aggregation_change_log_to_data(change_log)
        self._table("playbook_aggregation_change_logs").insert(data).execute()

    @handle_exceptions
    def get_playbook_aggregation_change_logs(
        self,
        playbook_name: str,
        agent_version: str,
        limit: int = 100,
    ) -> list[PlaybookAggregationChangeLog]:
        response = (
            self._table("playbook_aggregation_change_logs")
            .select("*")
            .eq("playbook_name", playbook_name)
            .eq("agent_version", agent_version)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        return response_list_to_playbook_aggregation_change_logs(_rows(response))

    @handle_exceptions
    def delete_all_playbook_aggregation_change_logs(self) -> None:
        self._table("playbook_aggregation_change_logs").delete().gte("id", 0).execute()
