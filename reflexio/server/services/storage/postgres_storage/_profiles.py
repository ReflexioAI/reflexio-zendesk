"""Profile and Interaction CRUD + search methods for Supabase storage."""

import logging
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any, cast

from reflexio.models.api_schema.retriever_schema import (
    SearchInteractionRequest,
    SearchUserProfileRequest,
)
from reflexio.models.api_schema.service_schemas import (
    DeleteUserInteractionRequest,
    DeleteUserProfileRequest,
    Interaction,
    Status,
    UserProfile,
)
from reflexio.server.llm.providers.embedding_service_provider import (
    EmbeddingUnavailableError,
)
from reflexio.server.services.storage.postgres_storage._opensearch import (
    status_filter_terms,
)
from reflexio.server.services.storage.postgres_storage._profile_converters import (
    interaction_to_data,
    response_list_to_interactions,
    response_list_to_user_profiles,
    user_profile_to_data,
)
from reflexio.server.usage_metrics import record_usage_event

from ._base import (
    _INTERACTION_COLUMNS,
    _PROFILE_COLUMNS,
    PostgresStorageBase,
    _apply_status_filter_to_query,
    _rows,
)
from ._protocols import SchemaScopedClient

logger = logging.getLogger(__name__)

handle_exceptions = PostgresStorageBase.handle_exceptions


class ProfileMixin(SchemaScopedClient):
    # Type hints for instance attributes/methods provided by PostgresStorageBase via MRO
    client: Any
    org_id: str
    _get_embedding: Any
    _should_expand_documents: Any
    _expand_document: Any
    llm_client: Any
    embedding_model_name: str
    embedding_dimensions: int
    search_mode: Any
    vector_weight: float
    fts_weight: float
    _opensearch: Any

    def _record_profile_event(
        self,
        *,
        event_name: str,
        outcome: str,
        entity_id: str | None,
        **extra: Any,
    ) -> None:
        record_usage_event(
            org_id=self.org_id,
            event_category="entity_change",
            event_name=event_name,
            outcome=outcome,
            entity_type="profile",
            entity_id=entity_id,
            **extra,
        )

    @handle_exceptions
    def get_all_profiles(
        self,
        limit: int = 100,
        status_filter: list[Status | None] | None = None,
    ) -> list[UserProfile]:
        if status_filter is None:
            status_filter = [None]  # Default to current profiles (status=None)

        query = self._table("profiles").select(_PROFILE_COLUMNS)

        # Convert Status enum values to strings for database query
        # Handle None values and Status.CURRENT (which has value None)
        status_strings = []
        has_none = False
        for status in status_filter:
            if status is None or (hasattr(status, "value") and status.value is None):
                has_none = True
            elif isinstance(status, Status):
                status_strings.append(status.value)
            elif isinstance(status, str):
                status_strings.append(status)

        # Build status filter: handle None and string values
        if has_none and status_strings:
            # Mix of None and other statuses: (status IS NULL OR status IN (...))
            query = query.or_(f"status.is.null,status.in.({','.join(status_strings)})")
        elif has_none:
            # Only None: status IS NULL
            query = query.is_("status", "null")
        else:
            # Only non-None statuses: status IN (...)
            query = query.in_("status", status_strings)

        response = (
            query.order("last_modified_timestamp", desc=True).limit(limit).execute()
        )
        return response_list_to_user_profiles(_rows(response))

    @handle_exceptions
    def get_user_profile(
        self,
        user_id: str,
        status_filter: list[Status | None] | None = None,
    ) -> list[UserProfile]:
        if status_filter is None:
            status_filter = [None]  # Default to current profiles (status=None)

        current_timestamp = int(datetime.now(UTC).timestamp())
        query = (
            self._table("profiles")
            .select(_PROFILE_COLUMNS)
            .eq("user_id", user_id)
            .gte("expiration_timestamp", current_timestamp)
        )

        # Convert Status enum values to strings for database query
        status_strings = []
        has_none = False
        for status in status_filter:
            if status is None or (hasattr(status, "value") and status.value is None):
                has_none = True
            elif isinstance(status, Status):
                status_strings.append(status.value)
            elif isinstance(status, str):
                status_strings.append(status)

        # Build status filter: handle None and string values
        if has_none and status_strings:
            # Mix of None and other statuses: (status IS NULL OR status IN (...))
            query = query.or_(f"status.is.null,status.in.({','.join(status_strings)})")
        elif has_none:
            # Only None: status IS NULL
            query = query.is_("status", "null")
        else:
            # Only non-None statuses: status IN (...)
            query = query.in_("status", status_strings)

        response = query.execute()
        return response_list_to_user_profiles(_rows(response))

    @handle_exceptions
    def add_user_profile(self, user_id: str, user_profiles: list[UserProfile]) -> None:  # noqa: ARG002
        for profile in user_profiles:
            embedding_text = "\n".join([profile.content, str(profile.custom_features)])
            if self._should_expand_documents():
                with ThreadPoolExecutor(max_workers=2) as executor:
                    emb_future = executor.submit(self._get_embedding, embedding_text)
                    exp_future = executor.submit(self._expand_document, profile.content)
                    profile.embedding = emb_future.result(timeout=15)
                    profile.expanded_terms = exp_future.result(timeout=15)
            else:
                profile.embedding = self._get_embedding(embedding_text)
            response = (
                self._table("profiles").upsert(user_profile_to_data(profile)).execute()
            )
            if self._opensearch:
                self._opensearch.index_rows("profiles", _rows(response))
            self._record_profile_event(
                event_name="profile_created",
                outcome="created",
                entity_id=profile.profile_id,
                user_id=profile.user_id or user_id,
                request_id=profile.generated_from_request_id,
                source=profile.source,
            )

    @handle_exceptions
    def update_user_profile_by_id(
        self, user_id: str, profile_id: str, new_profile: UserProfile
    ) -> None:
        current_timestamp = int(datetime.now(UTC).timestamp())
        response = (
            self._table("profiles")
            .select("profile_id")
            .eq("user_id", user_id)
            .eq("profile_id", profile_id)
            .gte("expiration_timestamp", current_timestamp)
            .execute()
        )

        if not response.data:
            logger.warning("User profile not found for user id: %s", user_id)
            return

        # Get embedding for the updated profile
        embedding = self._get_embedding(
            "\n".join([new_profile.content, str(new_profile.custom_features)])
        )
        new_profile.embedding = embedding
        response = (
            self._table("profiles")
            .update(user_profile_to_data(new_profile))
            .eq("profile_id", profile_id)
            .execute()
        )
        if self._opensearch:
            self._opensearch.index_rows("profiles", _rows(response))
        self._record_profile_event(
            event_name="profile_updated",
            outcome="updated",
            entity_id=profile_id,
            user_id=user_id,
            request_id=new_profile.generated_from_request_id,
            source=new_profile.source,
        )

    @handle_exceptions
    def delete_user_profile(self, request: DeleteUserProfileRequest) -> None:
        response = (
            self._table("profiles")
            .delete()
            .eq("user_id", request.user_id)
            .eq("profile_id", request.profile_id)
            .execute()
        )
        if not response.data:
            return
        if self._opensearch:
            self._opensearch.delete_ids(
                "profiles", [row.get("profile_id") for row in _rows(response)]
            )
        self._record_profile_event(
            event_name="profile_deleted",
            outcome="deleted",
            entity_id=request.profile_id,
            user_id=request.user_id,
        )

    @handle_exceptions
    def delete_all_profiles_for_user(self, user_id: str) -> None:
        self._table("profiles").delete().eq("user_id", user_id).execute()
        if self._opensearch:
            self._opensearch.delete_by_filter(
                "profiles", [{"term": {"user_id": user_id}}]
            )

    @handle_exceptions
    def delete_all_profiles(self) -> None:
        """Delete all profiles across all users."""
        self._delete_all_text_keyed("profiles", "profile_id")
        if self._opensearch:
            self._opensearch.delete_by_filter("profiles", [])

    @handle_exceptions
    def delete_profiles_by_ids(self, profile_ids: list[str]) -> int:
        """Delete profiles by their IDs."""
        if not profile_ids:
            return 0
        response = (
            self._table("profiles").delete().in_("profile_id", profile_ids).execute()
        )
        if self._opensearch:
            self._opensearch.delete_ids(
                "profiles", [row.get("profile_id") for row in _rows(response)]
            )
        return len(_rows(response))

    @handle_exceptions
    def get_profiles_by_ids(
        self,
        user_id: str,
        profile_ids: list[str],
        status_filter: list[Status | None] | None = None,
    ) -> list[UserProfile]:
        """Fetch selected current/non-current profiles for a user by id.

        See base class ``BaseStorage.get_profiles_by_ids`` for the full
        contract; this is the Supabase-backed implementation. The
        expiration filter is applied regardless of ``status_filter``,
        matching the SQLite implementation — expired rows are not
        returned even when explicitly asking for archived statuses.
        """
        if not profile_ids:
            return []
        if status_filter is None:
            status_filter = [None]

        current_timestamp = int(datetime.now(UTC).timestamp())
        query = (
            self._table("profiles")
            .select(_PROFILE_COLUMNS)
            .eq("user_id", user_id)
            .in_("profile_id", profile_ids)
            .gte("expiration_timestamp", current_timestamp)
        )
        query = _apply_status_filter_to_query(query, status_filter)

        response = query.execute()
        return response_list_to_user_profiles(_rows(response))

    @handle_exceptions
    def archive_profile_by_id(self, user_id: str, profile_id: str) -> bool:
        """Archive a single current profile, guarded by owner id.

        See base class ``BaseStorage.archive_profile_by_id`` for the full
        contract; this is the Supabase-backed implementation.
        """
        response = (
            self._table("profiles")
            .update(
                {
                    "status": Status.ARCHIVED.value,
                    "last_modified_timestamp": int(datetime.now(UTC).timestamp()),
                }
            )
            .eq("profile_id", profile_id)
            .eq("user_id", user_id)
            .is_("status", "null")
            .execute()
        )
        if self._opensearch:
            self._opensearch.index_rows("profiles", _rows(response))
        return len(_rows(response)) > 0

    @handle_exceptions
    def update_all_profiles_status(
        self,
        old_status: Status | None,
        new_status: Status | None,
        user_ids: list[str] | None = None,
    ) -> int:
        """
        Update all profiles with old_status to new_status atomically.

        Args:
            old_status: The current status to match (None for CURRENT)
            new_status: The new status to set (None for CURRENT)
            user_ids: Optional list of user_ids to filter updates. If None, updates all users.

        Returns:
            int: Number of profiles updated
        """
        # Build the query based on old_status
        # Update both status and last_modified_timestamp
        query = self._table("profiles").update(
            {
                "status": new_status.value if new_status else None,
                "last_modified_timestamp": int(datetime.now(UTC).timestamp()),
            }
        )

        if old_status is None or (
            hasattr(old_status, "value") and old_status.value is None
        ):
            # Match CURRENT profiles (status IS NULL)
            query = query.is_("status", "null")
        else:
            # Match specific status
            query = query.eq("status", old_status.value)

        # Add user_ids filter if provided
        if user_ids is not None:
            query = query.in_("user_id", user_ids)

        # Execute the update
        response = query.execute()
        if self._opensearch:
            self._opensearch.index_rows("profiles", _rows(response))

        # Count the number of rows updated
        updated_count = len(response.data) if response.data else 0
        logger.info(
            "Updated %s profiles from %s to %s",
            updated_count,
            old_status,
            new_status,
        )
        return updated_count

    @handle_exceptions
    def delete_all_profiles_by_status(self, status: Status) -> int:
        """
        Delete all profiles with the given status atomically.

        Args:
            status: The status of profiles to delete

        Returns:
            int: Number of profiles deleted
        """
        # Build the delete query
        query = self._table("profiles").delete().eq("status", status.value)

        # Execute the delete
        response = query.execute()
        if self._opensearch:
            self._opensearch.delete_ids(
                "profiles", [row.get("profile_id") for row in _rows(response)]
            )

        # Count the number of rows deleted
        deleted_count = len(response.data) if response.data else 0
        logger.info("Deleted %s profiles with status %s", deleted_count, status)
        return deleted_count

    @handle_exceptions
    def get_user_ids_with_status(self, status: Status | None) -> list[str]:
        """
        Get list of unique user_ids that have profiles with the given status.

        Args:
            status: The status to filter by (None for CURRENT)

        Returns:
            list[str]: List of unique user_ids
        """
        # Build the query to select distinct user_ids
        query = self._table("profiles").select("user_id")

        if status is None or (hasattr(status, "value") and status.value is None):
            # Match CURRENT profiles (status IS NULL)
            query = query.is_("status", "null")
        else:
            # Match specific status
            query = query.eq("status", status.value)

        # Execute the query
        response = query.execute()

        # Extract unique user_ids
        data = _rows(response)
        return list({row["user_id"] for row in data}) if data else []

    # ==============================
    # Interaction CRUD methods
    # ==============================

    @handle_exceptions
    def get_all_interactions(self, limit: int = 100) -> list[Interaction]:
        response = (
            self._table("interactions")
            .select(_INTERACTION_COLUMNS)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        return response_list_to_interactions(_rows(response))

    @handle_exceptions
    def get_user_interaction(self, user_id: str) -> list[Interaction]:
        response = (
            self._table("interactions")
            .select(_INTERACTION_COLUMNS)
            .eq("user_id", user_id)
            .execute()
        )
        return response_list_to_interactions(_rows(response))

    @handle_exceptions
    def add_user_interaction(self, user_id: str, interaction: Interaction) -> None:  # noqa: ARG002
        embedding = self._get_embedding(
            f"{interaction.content}\n{interaction.user_action_description}"
        )
        interaction.embedding = embedding
        response = (
            self._table("interactions")
            .upsert(interaction_to_data(interaction))
            .execute()
        )
        if self._opensearch:
            self._opensearch.index_rows("interactions", _rows(response))

    @handle_exceptions
    def add_user_interactions_bulk(
        self,
        user_id: str,  # noqa: ARG002
        interactions: list[Interaction],
    ) -> None:
        """
        Add multiple user interactions with batched embedding generation.

        This method generates embeddings for all interactions in a single API call,
        significantly reducing the number of embedding API calls when adding multiple
        interactions at once.

        Args:
            user_id: The user ID
            interactions: List of interactions to add
        """
        if not interactions:
            return

        # Prepare texts for batch embedding
        texts = [
            "\n".join(
                [interaction.content or "", interaction.user_action_description or ""]
            )
            for interaction in interactions
        ]

        # Get all embeddings in a single API call
        try:
            embeddings = self.llm_client.get_embeddings(
                texts, self.embedding_model_name, self.embedding_dimensions
            )
        except EmbeddingUnavailableError as exc:
            logger.warning(
                "Embedding unavailable for interaction bulk insert; "
                "continuing without vectors: %s",
                exc,
            )
            embeddings = [[] for _ in texts]

        # Assign embeddings to interactions
        for interaction, embedding in zip(interactions, embeddings, strict=False):
            interaction.embedding = embedding

        # Bulk upsert all interactions
        data_list = [interaction_to_data(interaction) for interaction in interactions]
        response = self._table("interactions").upsert(data_list).execute()
        if self._opensearch:
            self._opensearch.index_rows("interactions", _rows(response))

    @handle_exceptions
    def delete_user_interaction(self, request: DeleteUserInteractionRequest) -> None:
        response = (
            self._table("interactions")
            .delete()
            .eq("user_id", request.user_id)
            .eq("interaction_id", request.interaction_id)
            .execute()
        )
        if self._opensearch:
            self._opensearch.delete_ids(
                "interactions", [row.get("interaction_id") for row in _rows(response)]
            )

    @handle_exceptions
    def delete_all_interactions_for_user(self, user_id: str) -> None:
        self._table("interactions").delete().eq("user_id", user_id).execute()
        if self._opensearch:
            self._opensearch.delete_by_filter(
                "interactions", [{"term": {"user_id": user_id}}]
            )

    @handle_exceptions
    def delete_all_interactions(self) -> None:
        """Delete all interactions across all users."""
        self._table("interactions").delete().gte("interaction_id", 0).execute()
        if self._opensearch:
            self._opensearch.delete_by_filter("interactions", [])

    @handle_exceptions
    def count_all_interactions(self) -> int:
        """
        Count total interactions across all users.

        Returns:
            int: Total number of interactions
        """
        result = (
            self._table("interactions")
            .select("interaction_id", count="exact")  # type: ignore[reportArgumentType]
            .execute()
        )
        return result.count or 0

    @handle_exceptions
    def count_all_profiles(self) -> int:
        """
        Count total profiles across all users without hydrating rows.

        Returns:
            int: Total number of profiles across all users
        """
        result = (
            self._table("profiles")
            .select("profile_id", count="exact")  # type: ignore[reportArgumentType]
            .execute()
        )
        return result.count or 0

    @handle_exceptions
    def delete_oldest_interactions(self, count: int) -> int:
        """
        Delete the oldest N interactions based on created_at timestamp.

        Args:
            count (int): Number of oldest interactions to delete

        Returns:
            int: Number of interactions actually deleted
        """
        if count <= 0:
            return 0

        # Get oldest interaction IDs
        result = (
            self._table("interactions")
            .select("interaction_id")
            .order("created_at", desc=False)
            .limit(count)
            .execute()
        )
        rows = _rows(result)
        if not rows:
            return 0

        ids_to_delete = [row["interaction_id"] for row in rows]
        self._table("interactions").delete().in_(
            "interaction_id", ids_to_delete
        ).execute()
        return len(ids_to_delete)

    # ==============================
    # Search methods
    # ==============================

    @handle_exceptions
    def search_interaction(
        self,
        search_interaction_request: SearchInteractionRequest,
        query_embedding: list[float] | None = None,  # noqa: ARG002
    ) -> list[Interaction]:
        # Perform hybrid search (vector + FTS)
        if not search_interaction_request.query:
            return []

        effective_mode = search_interaction_request.search_mode or self.search_mode
        query_text = search_interaction_request.query
        if self._opensearch:
            filters: list[dict[str, Any]] = [
                {"term": {"user_id": search_interaction_request.user_id}}
            ]
            if search_interaction_request.request_id:
                filters.append(
                    {"term": {"request_id": search_interaction_request.request_id}}
                )
            if search_interaction_request.start_time:
                filters.append(
                    {
                        "range": {
                            "created_at": {
                                "gte": int(
                                    search_interaction_request.start_time.timestamp()
                                )
                            }
                        }
                    }
                )
            if search_interaction_request.end_time:
                filters.append(
                    {
                        "range": {
                            "created_at": {
                                "lte": int(
                                    search_interaction_request.end_time.timestamp()
                                )
                            }
                        }
                    }
                )
            ids = self._opensearch.search_ids(
                entity="interactions",
                query_text=query_text,
                query_embedding=query_embedding or self._get_embedding(query_text),
                search_mode=effective_mode,
                top_k=search_interaction_request.most_recent_k
                or search_interaction_request.top_k
                or 10,
                threshold=0.1,
                filters=filters,
            )
            interactions = cast(Any, self).get_interactions_by_ids(ids)
            return _order_by_ids(interactions, ids, "interaction_id")
        response = self._rpc(
            "hybrid_match_interactions",
            {
                "p_query_embedding": self._get_embedding(query_text),
                "p_query_text": query_text,
                "p_match_threshold": 0.1,
                "p_match_count": search_interaction_request.most_recent_k or 10,
                "p_search_mode": effective_mode.value,
                "p_rrf_k": 60,
                "p_vector_weight": self.vector_weight,
                "p_fts_weight": self.fts_weight,
            },
        ).execute()

        data = cast(list[dict[str, Any]], response.data)
        interactions = response_list_to_interactions(data)

        if search_interaction_request.most_recent_k:
            sorted_interactions = sorted(
                interactions, key=lambda x: x.created_at, reverse=True
            )
            return list(
                reversed(
                    sorted_interactions[: search_interaction_request.most_recent_k]
                )
            )
        return interactions

    @handle_exceptions
    def search_user_profile(
        self,
        search_user_profile_request: SearchUserProfileRequest,
        status_filter: Sequence[Status | None] | None = None,
        query_embedding: list[float] | None = None,
    ) -> list[UserProfile]:
        if status_filter is None:
            status_filter = [None]  # Default to current profiles (status=None)

        current_timestamp = int(datetime.now(UTC).timestamp())
        # Perform hybrid search (vector + FTS)
        if not search_user_profile_request.query:
            return []

        effective_mode = search_user_profile_request.search_mode or self.search_mode
        query_text = search_user_profile_request.query
        if self._opensearch:
            filters = [
                {"term": {"user_id": search_user_profile_request.user_id}},
                {"range": {"expiration_timestamp": {"gte": current_timestamp}}},
            ]
            terms = status_filter_terms(status_filter)
            if terms is not None:
                filters.append({"terms": {"status": terms}})
            if search_user_profile_request.source:
                filters.append({"term": {"source": search_user_profile_request.source}})
            if search_user_profile_request.extractor_name:
                filters.append(
                    {
                        "term": {
                            "extractor_names": search_user_profile_request.extractor_name
                        }
                    }
                )
            ids = self._opensearch.search_ids(
                entity="profiles",
                query_text=query_text,
                query_embedding=query_embedding or self._get_embedding(query_text),
                search_mode=effective_mode,
                top_k=search_user_profile_request.top_k or 10,
                threshold=search_user_profile_request.threshold or 0.7,
                filters=filters,
            )
            profiles = self.get_profiles_by_ids(
                search_user_profile_request.user_id,
                [str(profile_id) for profile_id in ids],
                status_filter=list(status_filter),
            )
            ordered_profiles = _order_by_ids(profiles, ids, "profile_id")
            if search_user_profile_request.custom_feature:
                custom_feature = search_user_profile_request.custom_feature.lower()
                ordered_profiles = [
                    profile
                    for profile in ordered_profiles
                    if custom_feature in str(profile.custom_features).lower()
                ]
            return ordered_profiles
        response = self._rpc(
            "hybrid_match_profiles",
            {
                "p_query_embedding": query_embedding or self._get_embedding(query_text),
                "p_query_text": query_text,
                "p_match_threshold": search_user_profile_request.threshold or 0.7,
                "p_match_count": search_user_profile_request.top_k or 10,
                "p_current_epoch": current_timestamp,
                "p_filter_user_id": search_user_profile_request.user_id,
                "p_search_mode": effective_mode.value,
                "p_rrf_k": 60,
                "p_filter_extractor_name": search_user_profile_request.extractor_name,
                "p_vector_weight": self.vector_weight,
                "p_fts_weight": self.fts_weight,
            },
        ).execute()

        data = cast(list[dict[str, Any]], response.data)
        profiles = response_list_to_user_profiles(data)
        filtered_profiles = []
        for profile in profiles:
            # Apply status filter - compare Status enum values
            profile_matches_filter = False
            for status in status_filter:
                if status is None or (
                    hasattr(status, "value") and status.value is None
                ):
                    # Filter includes None/CURRENT - check if profile status is None
                    if profile.status is None:
                        profile_matches_filter = True
                        break
                elif isinstance(status, Status):
                    # Compare enum values
                    if profile.status == status:
                        profile_matches_filter = True
                        break
                elif (
                    isinstance(status, str)
                    and profile.status
                    and profile.status.value == status
                ):
                    profile_matches_filter = True
                    break

            if not profile_matches_filter:
                continue

            if search_user_profile_request.source and (
                not profile.source
                or search_user_profile_request.source.lower() != profile.source.lower()
            ):
                continue
            if search_user_profile_request.custom_feature and (
                search_user_profile_request.custom_feature.lower()
                not in str(profile.custom_features).lower()
            ):
                continue
            filtered_profiles.append(profile)

        return filtered_profiles


def _order_by_ids(items: list[Any], ids: Sequence[Any], id_attr: str) -> list[Any]:
    by_id = {str(getattr(item, id_attr)): item for item in items}
    return [by_id[str(item_id)] for item_id in ids if str(item_id) in by_id]
