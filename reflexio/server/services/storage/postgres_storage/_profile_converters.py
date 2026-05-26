"""Converter functions for profile, interaction, and profile change log domain objects."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from reflexio.models.api_schema.service_schemas import (
    Interaction,
    ProfileChangeLog,
    ProfileTimeToLive,
    ToolUsed,
    UserActionType,
    UserProfile,
)
from reflexio.server.services.storage.postgres_storage._timestamp_utils import (
    _parse_iso_timestamp,
)


def response_to_user_profile(item: Mapping[str, Any]) -> UserProfile:
    """
    Convert a response item from Supabase to a UserProfile object.

    Args:
        item: Dictionary containing profile data from Supabase response

    Returns:
        UserProfile object
    """
    from reflexio.models.api_schema.service_schemas import Status

    return UserProfile(
        profile_id=item["profile_id"],
        user_id=item["user_id"],
        content=item["content"],
        last_modified_timestamp=item["last_modified_timestamp"],
        generated_from_request_id=item["generated_from_request_id"],
        profile_time_to_live=ProfileTimeToLive(item["profile_time_to_live"]),
        expiration_timestamp=item["expiration_timestamp"],
        custom_features=item["custom_features"],
        source=item.get("source", ""),
        status=Status(item["status"]) if item.get("status") else None,
        extractor_names=item.get("extractor_names"),
        expanded_terms=item.get("expanded_terms"),
        source_span=item.get("source_span"),
        notes=item.get("notes"),
        reader_angle=item.get("reader_angle"),
    )


def user_profile_to_data(profile: UserProfile) -> dict[str, Any]:
    """
    Convert a UserProfile object to data for upserting into Supabase.

    Args:
        profile: UserProfile object to convert

    Returns:
        Dictionary containing data ready for upsert
    """
    return {
        "profile_id": profile.profile_id,
        "user_id": profile.user_id,
        "content": profile.content,
        "last_modified_timestamp": profile.last_modified_timestamp,
        "generated_from_request_id": profile.generated_from_request_id,
        "profile_time_to_live": profile.profile_time_to_live.value,
        "expiration_timestamp": profile.expiration_timestamp,
        "custom_features": profile.custom_features,
        "embedding": profile.embedding,
        "source": profile.source,
        "status": profile.status.value if profile.status else None,
        "extractor_names": profile.extractor_names,
        "expanded_terms": profile.expanded_terms,
        "source_span": profile.source_span,
        "notes": profile.notes,
        "reader_angle": profile.reader_angle,
    }


def response_list_to_user_profiles(
    response_data: list[dict[str, Any]],
) -> list[UserProfile]:
    """
    Convert a list of response items to UserProfile objects.

    Args:
        response_data: List of dictionaries containing profile data from Supabase response

    Returns:
        List of UserProfile objects
    """
    return [response_to_user_profile(item) for item in response_data]


def response_to_interaction(item: Mapping[str, Any]) -> Interaction:
    """
    Convert a response item from Supabase to an Interaction object.

    Args:
        item: Dictionary containing interaction data from Supabase response

    Returns:
        Interaction object
    """
    # Deserialize tools_used from JSONB array
    tools_used = []
    tools_used_data = item.get("tools_used")
    if tools_used_data and isinstance(tools_used_data, list):
        tools_used = [ToolUsed(**t) for t in tools_used_data if isinstance(t, dict)]

    return Interaction(
        interaction_id=item["interaction_id"],
        user_id=item["user_id"],
        content=item["content"],
        request_id=item["request_id"],
        created_at=_parse_iso_timestamp(item["created_at"]),
        role=item.get("role", "User"),
        user_action=UserActionType(item["user_action"]),
        user_action_description=item["user_action_description"],
        interacted_image_url=item["interacted_image_url"],
        shadow_content=item.get("shadow_content") or "",
        expert_content=item.get("expert_content") or "",
        tools_used=tools_used,
    )


def interaction_to_data(interaction: Interaction) -> dict[str, Any]:
    """
    Convert an Interaction object to data for upserting into Supabase.

    Args:
        interaction: Interaction object to convert

    Returns:
        Dictionary containing data ready for upsert
    """
    data = {
        "user_id": interaction.user_id,
        "content": interaction.content,
        "request_id": interaction.request_id,
        "created_at": datetime.fromtimestamp(
            interaction.created_at, tz=UTC
        ).isoformat(),
        "role": interaction.role,
        "user_action": interaction.user_action.value,
        "user_action_description": interaction.user_action_description,
        "interacted_image_url": interaction.interacted_image_url,
        "shadow_content": interaction.shadow_content,
        "expert_content": interaction.expert_content,
        "tools_used": [t.model_dump() for t in interaction.tools_used],
        "embedding": interaction.embedding,
    }
    # Only include interaction_id if it's set (non-zero), otherwise let DB auto-generate
    if interaction.interaction_id:
        data["interaction_id"] = interaction.interaction_id
    return data


def response_list_to_interactions(
    response_data: list[dict[str, Any]],
) -> list[Interaction]:
    """
    Convert a list of response items to Interaction objects.

    Args:
        response_data: List of dictionaries containing interaction data from Supabase response

    Returns:
        List of Interaction objects
    """
    return [response_to_interaction(item) for item in response_data]


def response_to_profile_change_log(item: Mapping[str, Any]) -> ProfileChangeLog:
    """
    Convert a response item from Supabase to a ProfileChangeLog object.

    Args:
        item: Dictionary containing profile change log data from Supabase response

    Returns:
        ProfileChangeLog object
    """
    return ProfileChangeLog(
        id=item["id"],
        user_id=item["user_id"],
        request_id=item["request_id"],
        created_at=item["created_at"],  # Already an integer timestamp
        added_profiles=[UserProfile(**profile) for profile in item["added_profiles"]],
        removed_profiles=[
            UserProfile(**profile) for profile in item["removed_profiles"]
        ],
        mentioned_profiles=[
            UserProfile(**profile) for profile in item["mentioned_profiles"]
        ],
    )


def profile_change_log_to_data(profile_change_log: ProfileChangeLog) -> dict[str, Any]:
    """
    Convert a ProfileChangeLog object to data for upserting into Supabase.

    Args:
        profile_change_log: ProfileChangeLog object to convert

    Returns:
        Dictionary containing data ready for upsert
    """
    # skip id as it is auto generated by supabase
    return {
        "user_id": profile_change_log.user_id,
        "request_id": profile_change_log.request_id,
        "created_at": profile_change_log.created_at,
        "added_profiles": [
            profile.model_dump() for profile in profile_change_log.added_profiles
        ],
        "removed_profiles": [
            profile.model_dump() for profile in profile_change_log.removed_profiles
        ],
        "mentioned_profiles": [
            profile.model_dump() for profile in profile_change_log.mentioned_profiles
        ],
    }


def response_list_to_profile_change_logs(
    response_data: list[dict[str, Any]],
) -> list[ProfileChangeLog]:
    """
    Convert a list of response items to ProfileChangeLog objects.

    Args:
        response_data: List of dictionaries containing profile change log data from Supabase response

    Returns:
        List of ProfileChangeLog objects
    """
    return [response_to_profile_change_log(item) for item in response_data]
