"""Native Postgres storage with direct SQL and pgvector search."""

from reflexio.server.services.storage.postgres_storage._extras import ExtrasMixin
from reflexio.server.services.storage.postgres_storage._operations import (
    OperationMixin,
)
from reflexio.server.services.storage.postgres_storage._playbook import (
    PlaybookMixin,
)
from reflexio.server.services.storage.postgres_storage._profiles import ProfileMixin
from reflexio.server.services.storage.postgres_storage._requests import RequestMixin
from reflexio.server.services.storage.postgres_storage._share_links import (
    PostgresShareLinkMixin,
)

from ._base import PostgresStorageBase


class PostgresStorage(
    ProfileMixin,
    RequestMixin,
    PlaybookMixin,
    OperationMixin,
    ExtrasMixin,
    PostgresShareLinkMixin,
    PostgresStorageBase,
):
    """PostgreSQL storage with direct SQL access."""

    pass


__all__ = ["PostgresStorage"]
