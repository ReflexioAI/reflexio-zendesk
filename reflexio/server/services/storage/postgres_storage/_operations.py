"""Operation state methods for Supabase storage."""

import logging
from datetime import UTC, datetime
from typing import Any, cast

from reflexio.models.api_schema.internal_schema import RequestInteractionDataModel
from reflexio.models.api_schema.service_schemas import (
    Interaction,
    Request,
)

from ._base import (
    _OPERATION_STATE_COLUMNS,
    PostgresStorageBase,
    _parse_rpc_row_to_interaction,
    _parse_rpc_row_to_request,
    _rows,
)
from ._protocols import SchemaScopedClient

logger = logging.getLogger(__name__)

handle_exceptions = PostgresStorageBase.handle_exceptions


class OperationMixin(SchemaScopedClient):
    # Type hints for instance attributes/methods provided by PostgresStorageBase via MRO
    client: Any
    _current_timestamp: Any

    # ==============================
    # Operation State methods
    # ==============================

    @handle_exceptions
    def create_operation_state(self, service_name: str, operation_state: dict) -> None:
        """
        Create operation state for a service.

        Args:
            service_name (str): Name of the service
            operation_state (dict): Operation state data as a dictionary
        """
        data = {
            "service_name": service_name,
            "operation_state": operation_state,
            "updated_at": self._current_timestamp(),
        }
        self._table("_operation_state").insert(data).execute()

    @handle_exceptions
    def upsert_operation_state(self, service_name: str, operation_state: dict) -> None:
        """
        Create or update operation state for a service.

        Args:
            service_name (str): Name of the service
            operation_state (dict): Operation state data as a dictionary
        """
        data = {
            "service_name": service_name,
            "operation_state": operation_state,
            "updated_at": self._current_timestamp(),
        }
        self._table("_operation_state").upsert(data).execute()

    @handle_exceptions
    def get_operation_state(self, service_name: str) -> dict | None:
        """
        Get operation state for a specific service.

        Args:
            service_name (str): Name of the service

        Returns:
            Optional[dict]: Operation state data or None if not found
        """
        response = (
            self._table("_operation_state")
            .select(_OPERATION_STATE_COLUMNS)
            .eq("service_name", service_name)
            .execute()
        )

        data = _rows(response)
        if data:
            return {
                "service_name": data[0]["service_name"],
                "operation_state": data[0]["operation_state"],
                "updated_at": data[0]["updated_at"],
            }
        return None

    @handle_exceptions
    def get_operation_state_with_new_request_interaction(
        self,
        service_name: str,
        user_id: str | None,
        sources: list[str] | None = None,
    ) -> tuple[dict, list[RequestInteractionDataModel]]:
        """
        Retrieve operation state and new interactions grouped by request using a single SQL query.

        Uses an RPC function to perform JOIN and filtering in the database for efficiency.

        Args:
            service_name (str): Name of the service
            user_id (Optional[str]): User identifier to filter interactions.
                If None, returns interactions across all users (for non-user-scoped extractors).
            sources (Optional[list[str]]): Optional list of sources to filter interactions by

        Returns:
            tuple[dict, list[RequestInteractionDataModel]]: Operation state payload and list of
                RequestInteractionDataModel objects containing new interactions grouped by request
        """
        # Query 1: Get operation state
        state_record = self.get_operation_state(service_name)
        operation_state: dict = {}
        if state_record and isinstance(state_record.get("operation_state"), dict):
            operation_state = state_record["operation_state"]

        # Extract filtering params
        last_processed_ids = operation_state.get("last_processed_interaction_ids") or []
        last_processed_timestamp = operation_state.get("last_processed_timestamp")

        # Convert timestamp to ISO format for SQL
        timestamp_iso = None
        if last_processed_timestamp is not None:
            timestamp_iso = datetime.fromtimestamp(
                last_processed_timestamp, tz=UTC
            ).isoformat()

        # Query 2: Call RPC function for JOIN and filtering in database
        response = self._rpc(
            "get_new_request_interaction_groups",
            {
                "p_user_id": user_id,
                "p_last_processed_timestamp": timestamp_iso,
                "p_excluded_interaction_ids": [int(id) for id in last_processed_ids],
                "p_sources": sources,
            },
        ).execute()

        # Group results by request_id
        requests_map: dict[str, Request] = {}
        interactions_by_request: dict[str, list[Interaction]] = {}
        data = cast(list[dict[str, Any]], response.data)

        for row in data:
            req_id = row["request_id"]

            # Build Request object (once per request)
            if req_id not in requests_map:
                requests_map[req_id] = _parse_rpc_row_to_request(row)
                interactions_by_request[req_id] = []

            interaction = _parse_rpc_row_to_interaction(row)
            interactions_by_request[req_id].append(interaction)

        # Build RequestInteractionDataModel objects
        sessions: list[RequestInteractionDataModel] = []
        for req_id, req in requests_map.items():
            interactions = sorted(
                interactions_by_request[req_id], key=lambda x: x.created_at or 0
            )
            group_name = req.session_id or req.request_id
            sessions.append(
                RequestInteractionDataModel(
                    session_id=group_name,
                    request=req,
                    interactions=interactions,
                )
            )

        # Sort groups by earliest interaction
        sessions.sort(
            key=lambda g: (
                min(i.created_at or 0 for i in g.interactions) if g.interactions else 0
            )
        )

        return operation_state, sessions

    @handle_exceptions
    def get_last_k_interactions_grouped(
        self,
        user_id: str | None,
        k: int,
        sources: list[str] | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        agent_version: str | None = None,
    ) -> tuple[list[RequestInteractionDataModel], list[Interaction]]:
        """
        Get the last K interactions ordered by time (most recent first), grouped by request.

        Uses an RPC function to efficiently retrieve the last K interactions and their
        associated request data in a single database query.

        Args:
            user_id (Optional[str]): User identifier to filter interactions.
                If None, returns interactions across all users (for non-user-scoped extractors).
            k (int): Maximum number of interactions to retrieve
            sources (Optional[list[str]]): Optional list of sources to filter interactions by.
                If provided, only interactions from requests with source in this list are returned.
            start_time (Optional[int]): Unix timestamp. Only return interactions created at or after this time.
            end_time (Optional[int]): Unix timestamp. Only return interactions created at or before this time.
            agent_version (Optional[str]): Filter by agent_version on the request.
                If provided, only interactions from requests with this agent_version are returned.

        Returns:
            tuple[list[RequestInteractionDataModel], list[Interaction]]:
                - List of RequestInteractionDataModel objects (grouped by request/session)
                - Flat list of all interactions sorted by created_at DESC
        """
        # Call RPC function for efficient retrieval
        response = self._rpc(
            "get_last_k_interactions",
            {
                "p_user_id": user_id,
                "p_limit": k,
                "p_sources": sources,
                "p_start_time": start_time,
                "p_end_time": end_time,
                "p_agent_version": agent_version,
            },
        ).execute()

        # Build flat interactions list and group by request
        flat_interactions: list[Interaction] = []
        requests_map: dict[str, Request] = {}
        interactions_by_request: dict[str, list[Interaction]] = {}
        data = cast(list[dict[str, Any]], response.data)

        for row in data:
            req_id = row["request_id"]

            if req_id not in requests_map:
                requests_map[req_id] = _parse_rpc_row_to_request(row)
                interactions_by_request[req_id] = []

            interaction = _parse_rpc_row_to_interaction(row)
            flat_interactions.append(interaction)
            interactions_by_request[req_id].append(interaction)

        # Build RequestInteractionDataModel objects
        sessions: list[RequestInteractionDataModel] = []
        for req_id, req in requests_map.items():
            # Sort interactions by created_at ASC within each group
            interactions = sorted(
                interactions_by_request[req_id], key=lambda x: x.created_at or 0
            )
            group_name = req.session_id or req.request_id
            sessions.append(
                RequestInteractionDataModel(
                    session_id=group_name,
                    request=req,
                    interactions=interactions,
                )
            )

        # Sort groups by earliest interaction timestamp
        sessions.sort(
            key=lambda g: (
                min(i.created_at or 0 for i in g.interactions) if g.interactions else 0
            )
        )

        return sessions, flat_interactions

    @handle_exceptions
    def update_operation_state(self, service_name: str, operation_state: dict) -> None:
        """
        Update operation state for a specific service.

        Args:
            service_name (str): Name of the service
            operation_state (dict): Operation state data as a dictionary
        """
        data = {
            "operation_state": operation_state,
            "updated_at": self._current_timestamp(),
        }
        self._table("_operation_state").update(data).eq(
            "service_name", service_name
        ).execute()

    @handle_exceptions
    def get_all_operation_states(self) -> list[dict]:
        """
        Get all operation states.

        Returns:
            list[dict]: List of all operation state records
        """
        response = (
            self._table("_operation_state").select(_OPERATION_STATE_COLUMNS).execute()
        )
        data = cast(list[dict[str, Any]], response.data)
        return [
            {
                "service_name": item["service_name"],
                "operation_state": item["operation_state"],
                "updated_at": item["updated_at"],
            }
            for item in data
        ]

    @handle_exceptions
    def delete_operation_state(self, service_name: str) -> None:
        """
        Delete operation state for a specific service.

        Args:
            service_name (str): Name of the service
        """
        self._table("_operation_state").delete().eq(
            "service_name", service_name
        ).execute()

    @handle_exceptions
    def delete_all_operation_states(self) -> None:
        """Delete all operation states."""
        self._delete_all_text_keyed("_operation_state", "service_name")

    @handle_exceptions
    def try_acquire_in_progress_lock(
        self,
        state_key: str,
        request_id: str,
        stale_lock_seconds: int = 300,
        payload: dict | None = None,
    ) -> dict:
        """
        Atomically try to acquire an in-progress lock using PostgreSQL RPC.

        This method uses a single atomic database operation to either:
        1. Acquire the lock if no active lock exists (or lock is stale)
        2. Append ``{"request_id": request_id, "payload": payload}`` to the
           ``pending_request_queue`` if held by another request, dropping
           duplicates so publish retries are idempotent
           (R2 / reflexio-enterprise#59).

        Args:
            state_key (str): The operation state key (e.g., "profile_generation_in_progress::3::user_id")
            request_id (str): The current request's unique identifier
            stale_lock_seconds (int): Seconds after which a lock is considered stale (default 300)
            payload (dict | None): Optional serialized request payload to enqueue
                so the rerun runs against the SAME interactions the blocked
                publish enqueued.

        Returns:
            dict: Result with keys:
                - 'acquired' (bool): True if lock was acquired, False if blocked
                - 'state' (dict): The current operation state after the operation
        """
        response = self._rpc(
            "try_acquire_in_progress_lock",
            {
                "p_state_key": state_key,
                "p_request_id": request_id,
                "p_stale_lock_seconds": stale_lock_seconds,
                "p_payload": payload if payload is not None else {},
            },
        ).execute()

        if response.data and isinstance(response.data, dict):
            return response.data
        return {"acquired": False, "state": {}}

    @handle_exceptions
    def clear_in_progress_lock_if_owner(
        self,
        state_key: str,
        request_id: str,
        cleared_state: dict,
    ) -> bool:
        response = (
            self._table("_operation_state")
            .select(_OPERATION_STATE_COLUMNS)
            .eq("service_name", state_key)
            .execute()
        )
        rows = _rows(response)
        if not rows:
            return False
        state = rows[0].get("operation_state") or {}
        if state.get("current_request_id") != request_id and state.get(
            "request_id"
        ) != request_id:
            return False
        self.upsert_operation_state(state_key, cleared_state)
        return True
