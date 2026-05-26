"""Supabase implementation of ShareLinkMixin."""

import logging
import time
from typing import Any

from reflexio.models.api_schema.domain import ShareLink

from ._base import PostgresStorageBase, _rows
from ._protocols import SchemaScopedClient

logger = logging.getLogger(__name__)

handle_exceptions = PostgresStorageBase.handle_exceptions


class PostgresShareLinkMixin(SchemaScopedClient):
    """Supabase-backed share link operations."""

    # Type hints for instance attributes/methods provided by PostgresStorageBase via MRO
    client: Any
    org_id: str

    @handle_exceptions
    def create_share_link(
        self,
        token: str,
        resource_type: str,
        resource_id: str,
        expires_at: int | None,
        created_by_email: str | None,
    ) -> ShareLink:
        """Create a new share link record.

        Args:
            token (str): Unique token for the share link
            resource_type (str): Type of the resource being shared
            resource_id (str): ID of the resource being shared
            expires_at (int | None): Optional expiry timestamp (Unix epoch)
            created_by_email (str | None): Optional email of the creator

        Returns:
            ShareLink: The newly created share link
        """
        now = int(time.time())
        data = {
            "org_id": self.org_id,
            "token": token,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "created_at": now,
            "expires_at": expires_at,
            "created_by_email": created_by_email,
        }
        response = self._table("share_links").insert(data).execute()
        return _row_to_share_link(_rows(response)[0])

    @handle_exceptions
    def get_share_link_by_token(self, token: str) -> ShareLink | None:
        """Get a share link by its token, scoped to this org.

        Args:
            token (str): The share link token to look up

        Returns:
            ShareLink | None: The share link if found, otherwise None
        """
        response = (
            self._table("share_links")
            .select("*")
            .eq("org_id", self.org_id)
            .eq("token", token)
            .execute()
        )
        rows = _rows(response)
        if rows:
            return _row_to_share_link(rows[0])
        return None

    @handle_exceptions
    def get_share_link_by_resource(
        self, resource_type: str, resource_id: str
    ) -> ShareLink | None:
        """Get a share link for a specific resource, scoped to this org.

        Args:
            resource_type (str): Type of the resource
            resource_id (str): ID of the resource

        Returns:
            ShareLink | None: The share link if found, otherwise None
        """
        response = (
            self._table("share_links")
            .select("*")
            .eq("org_id", self.org_id)
            .eq("resource_type", resource_type)
            .eq("resource_id", resource_id)
            .limit(1)
            .execute()
        )
        rows = _rows(response)
        if rows:
            return _row_to_share_link(rows[0])
        return None

    @handle_exceptions
    def get_share_links(self) -> list[ShareLink]:
        """Get all share links for this org.

        Returns:
            list[ShareLink]: All share links for this org, ordered by creation time
        """
        response = (
            self._table("share_links")
            .select("*")
            .eq("org_id", self.org_id)
            .order("created_at")
            .execute()
        )
        return [_row_to_share_link(r) for r in (_rows(response) or [])]

    @handle_exceptions
    def delete_share_link(self, link_id: int) -> bool:
        """Delete a share link by ID, scoped to this org.

        Args:
            link_id (int): The share link ID to delete

        Returns:
            bool: True if a row was deleted, False otherwise
        """
        response = (
            self._table("share_links")
            .delete()
            .eq("org_id", self.org_id)
            .eq("id", link_id)
            .execute()
        )
        return bool(_rows(response))

    @handle_exceptions
    def delete_all_share_links(self) -> int:
        """Delete all share links for this org.

        Returns:
            int: Number of share links deleted
        """
        response = (
            self._table("share_links")
            .delete()
            .eq("org_id", self.org_id)
            .neq("id", 0)
            .execute()
        )
        rows = _rows(response)
        return len(rows) if rows else 0


def _row_to_share_link(row: dict[str, Any]) -> ShareLink:
    """Convert a database row dict to a ShareLink model.

    Args:
        row (dict[str, Any]): Raw database row from Supabase

    Returns:
        ShareLink: Parsed share link model
    """
    return ShareLink(
        id=row["id"],
        org_id=row["org_id"],
        token=row["token"],
        resource_type=row["resource_type"],
        resource_id=row["resource_id"],
        created_at=row.get("created_at"),
        expires_at=row.get("expires_at"),
        created_by_email=row.get("created_by_email"),
    )
