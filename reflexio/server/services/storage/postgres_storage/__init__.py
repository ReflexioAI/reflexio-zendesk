"""Native Postgres storage with direct SQL and pgvector search."""

from reflexio.server.services.storage.postgres_storage._agent_run import (
    PostgresAgentRunMixin,
)
from reflexio.server.services.storage.postgres_storage._extras import ExtrasMixin
from reflexio.server.services.storage.postgres_storage._governance import (
    PostgresGovernanceMixin,
)
from reflexio.server.services.storage.postgres_storage._lineage import (
    PostgresLineageMixin,
)
from reflexio.server.services.storage.postgres_storage._operations import (
    OperationMixin,
)
from reflexio.server.services.storage.postgres_storage._playbook import (
    PlaybookMixin,
)
from reflexio.server.services.storage.postgres_storage._profiles import ProfileMixin
from reflexio.server.services.storage.postgres_storage._requests import RequestMixin
from reflexio.server.services.storage.postgres_storage._retrieval_log import (
    PostgresRetrievalLogMixin,
)
from reflexio.server.services.storage.postgres_storage._shadow_verdicts import (
    PostgresShadowVerdictsMixin,
)
from reflexio.server.services.storage.postgres_storage._share_links import (
    PostgresShareLinkMixin,
)
from reflexio.server.services.storage.postgres_storage._stall_state import (
    PostgresStallStateMixin,
)

from ._base import PostgresStorageBase


class PostgresStorage(
    PostgresAgentRunMixin,
    ProfileMixin,
    RequestMixin,
    PlaybookMixin,
    PostgresRetrievalLogMixin,
    PostgresGovernanceMixin,
    PostgresLineageMixin,
    OperationMixin,
    ExtrasMixin,
    PostgresShareLinkMixin,
    PostgresStallStateMixin,
    PostgresShadowVerdictsMixin,
    PostgresStorageBase,
):
    """PostgreSQL storage with direct SQL access."""

    pass


__all__ = ["PostgresStorage"]
