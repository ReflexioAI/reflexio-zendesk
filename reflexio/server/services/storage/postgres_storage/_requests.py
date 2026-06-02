"""Request CRUD methods for Supabase storage."""

import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from reflexio.models.api_schema.internal_schema import RequestInteractionDataModel
from reflexio.models.api_schema.service_schemas import (
    Interaction,
    Request,
)
from reflexio.server.services.storage.postgres_storage._migration_utils import (
    request_to_data,
    response_to_request,
)
from reflexio.server.services.storage.postgres_storage._profile_converters import (
    response_to_interaction,
)

from ._base import (
    _INTERACTION_COLUMNS,
    _REQUEST_COLUMNS,
    PostgresStorageBase,
    _rows,
)
from ._protocols import SchemaScopedClient

logger = logging.getLogger(__name__)

handle_exceptions = PostgresStorageBase.handle_exceptions


class RequestMixin(SchemaScopedClient):
    # Type hints for instance attributes/methods provided by PostgresStorageBase via MRO
    client: Any
    _opensearch: Any

    # ==============================
    # Request methods
    # ==============================

    @handle_exceptions
    def add_request(self, request: Request) -> None:
        """
        Add a request to storage.

        Args:
            request: Request object to store
        """
        self._table("requests").upsert(request_to_data(request)).execute()

    @handle_exceptions
    def get_request(self, request_id: str) -> Request | None:
        """
        Get a request by its ID.

        Args:
            request_id: The request ID to retrieve

        Returns:
            Request object if found, None otherwise
        """
        response = (
            self._table("requests")
            .select(_REQUEST_COLUMNS)
            .eq("request_id", request_id)
            .execute()
        )

        data = _rows(response)
        if not data:
            return None

        return response_to_request(data[0])

    @handle_exceptions
    def delete_request(self, request_id: str) -> None:
        """
        Delete a request by its ID and all associated interactions.

        Args:
            request_id: The request ID to delete
        """
        # First delete all interactions associated with this request
        self._table("interactions").delete().eq("request_id", request_id).execute()
        if self._opensearch:
            self._opensearch.delete_by_filter(
                "interactions", [{"term": {"request_id": request_id}}]
            )
        # Then delete the request itself
        self._table("requests").delete().eq("request_id", request_id).execute()

    @handle_exceptions
    def delete_session(self, session_id: str) -> int:
        """
        Delete all requests and interactions in a session.

        Args:
            session_id: The session ID to delete

        Returns:
            int: Number of requests deleted
        """
        # First get all request IDs in this session
        response = (
            self._table("requests")
            .select("request_id")
            .eq("session_id", session_id)
            .execute()
        )

        data = _rows(response)
        if not data:
            return 0

        request_ids = [r["request_id"] for r in data]
        request_count = len(request_ids)

        # Delete all interactions for all requests in this session
        for request_id in request_ids:
            self._table("interactions").delete().eq("request_id", request_id).execute()
        if self._opensearch:
            self._opensearch.delete_by_filter(
                "interactions", [{"terms": {"request_id": request_ids}}]
            )

        # Delete all requests in this session
        self._table("requests").delete().eq("session_id", session_id).execute()

        return request_count

    @handle_exceptions
    def delete_all_requests(self) -> None:
        """Delete all requests and their associated interactions."""
        # First delete all interactions
        self._delete_all_text_keyed("interactions", "request_id")
        if self._opensearch:
            self._opensearch.delete_by_filter("interactions", [])
        # Then delete all requests
        self._delete_all_text_keyed("requests", "request_id")

    @handle_exceptions
    def delete_requests_by_ids(self, request_ids: Sequence[str]) -> int:
        """Delete requests and their associated interactions by request IDs."""
        if not request_ids:
            return 0
        # First delete all interactions for these requests
        self._table("interactions").delete().in_("request_id", request_ids).execute()
        if self._opensearch:
            self._opensearch.delete_by_filter(
                "interactions", [{"terms": {"request_id": list(request_ids)}}]
            )
        # Then delete the requests
        response = (
            self._table("requests").delete().in_("request_id", request_ids).execute()
        )
        return len(_rows(response))

    @handle_exceptions
    def get_requests_by_session(self, user_id: str, session_id: str) -> list[Request]:
        """
        Get all requests for a specific session.

        Args:
            user_id (str): User ID to filter requests
            session_id (str): Session ID to filter by

        Returns:
            list[Request]: List of Request objects in the session
        """
        response = (
            self._table("requests")
            .select(_REQUEST_COLUMNS)
            .eq("user_id", user_id)
            .eq("session_id", session_id)
            .execute()
        )

        data = _rows(response)
        if not data:
            return []

        return [response_to_request(item) for item in data]

    @handle_exceptions
    def get_sessions(
        self,
        user_id: str | None = None,
        request_id: str | None = None,
        session_id: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        top_k: int | None = 30,
        offset: int = 0,
    ) -> dict[str, list[RequestInteractionDataModel]]:
        """
        Get requests with their associated interactions, grouped by session_id.

        Uses PostgREST's automatic JOIN syntax via the foreign key relationship between
        requests and interactions tables. Applies request-level filters and pagination,
        then groups returned requests by session_id.

        Args:
            user_id (str, optional): User ID to filter requests.
            request_id (str, optional): Specific request ID to retrieve
            session_id (str, optional): Specific session ID to retrieve
            start_time (int, optional): Start timestamp for filtering
            end_time (int, optional): End timestamp for filtering
            top_k (int, optional): Maximum number of requests to return
            offset (int): Number of requests to skip for pagination

        Returns:
            dict[str, list[RequestInteractionDataModel]]: Dictionary mapping session_id to list of RequestInteractionDataModel objects
        """
        select_expr = f"*, interactions({_INTERACTION_COLUMNS})"
        query = (
            self._table("requests").select(select_expr).order("created_at", desc=True)
        )

        # Apply user_id filter if specified
        if user_id:
            query = query.eq("user_id", user_id)

        # Apply filters
        if request_id:
            query = query.eq("request_id", request_id)
        if session_id:
            query = query.eq("session_id", session_id)
        if start_time:
            start_time_iso = datetime.fromtimestamp(start_time, tz=UTC).isoformat()
            query = query.gte("created_at", start_time_iso)
        if end_time:
            end_time_iso = datetime.fromtimestamp(end_time, tz=UTC).isoformat()
            query = query.lte("created_at", end_time_iso)

        # Apply pagination: limit and offset on filtered requests.
        effective_limit = top_k or 100
        query = query.limit(effective_limit)
        if offset:
            query = query.offset(offset)

        response = query.execute()

        data = _rows(response)
        if not data:
            return {}

        # Parse and group the results
        grouped_results = {}
        for item in data:
            # Parse request
            req = response_to_request(item)

            # Get the group name
            group_name = req.session_id or ""

            # Parse interactions
            interactions: list[Interaction] = []
            if item.get("interactions"):
                # Handle both single interaction and array of interactions
                interaction_data = item["interactions"]
                if isinstance(interaction_data, list):
                    interactions.extend(
                        response_to_interaction(int_data)
                        for int_data in interaction_data
                    )
                else:
                    # Single interaction case
                    interactions.append(response_to_interaction(interaction_data))

            # Sort interactions by created_at
            interactions = sorted(interactions, key=lambda x: x.created_at)

            # Add to grouped results
            if group_name not in grouped_results:
                grouped_results[group_name] = []
            grouped_results[group_name].append(
                RequestInteractionDataModel(
                    session_id=group_name,
                    request=req,
                    interactions=interactions,
                )
            )

        return grouped_results

    @handle_exceptions
    def get_rerun_user_ids(
        self,
        user_id: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        source: str | None = None,
        agent_version: str | None = None,
    ) -> list[str]:
        """
        Get distinct user IDs that have matching requests for rerun workflows.

        Args:
            user_id (str, optional): Restrict to a specific user ID.
            start_time (int, optional): Start timestamp for request filtering.
            end_time (int, optional): End timestamp for request filtering.
            source (str, optional): Restrict to requests from a source.
            agent_version (str, optional): Restrict to requests with an agent version.

        Returns:
            list[str]: Distinct user IDs matching the filters.
        """
        page_size = 1000
        current_offset = 0
        user_ids: set[str] = set()

        while True:
            query = (
                self._table("requests")
                .select("user_id")
                .order("created_at", desc=True)
                .limit(page_size)
                .offset(current_offset)
            )

            if user_id:
                query = query.eq("user_id", user_id)
            if start_time:
                start_time_iso = datetime.fromtimestamp(start_time, tz=UTC).isoformat()
                query = query.gte("created_at", start_time_iso)
            if end_time:
                end_time_iso = datetime.fromtimestamp(end_time, tz=UTC).isoformat()
                query = query.lte("created_at", end_time_iso)
            if source:
                query = query.eq("source", source)
            if agent_version:
                query = query.eq("agent_version", agent_version)

            response = query.execute()
            rows = _rows(response)

            if not rows:
                break

            for row in rows:
                row_user_id = row.get("user_id")
                if row_user_id:
                    user_ids.add(row_user_id)

            if len(rows) < page_size:
                break
            current_offset += page_size

        return sorted(user_ids)
