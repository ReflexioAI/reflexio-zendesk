"""
Data migration functions that run alongside SQL schema migrations.

Each function receives (conn, cursor) and must NOT commit or rollback —
the caller (execute_migration) manages the transaction.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from typing import TYPE_CHECKING

import psycopg2.extensions

from reflexio.server.llm.providers.embedding_service_provider import (
    EmbeddingUnavailableError,
)

if TYPE_CHECKING:
    from reflexio.server.llm.litellm_client import LiteLLMClient

logger = logging.getLogger(__name__)

DataMigrationFn = Callable[
    [psycopg2.extensions.connection, psycopg2.extensions.cursor, str], None
]

# Pattern 1: Legacy structured format (formerly produced by format_structured_feedback_content)
#   When: "condition"
#   Do: "action"
#   Don't: "avoid action"
_STRUCTURED_RE = re.compile(
    r'When:\s*"(?P<when>.+?)"'
    r'(?:\s*Do:\s*"(?P<do>.+?)")?'
    r"(?:\s*Don'?t:\s*\"(?P<dont>.+?)\")?",
    re.DOTALL,
)

# Pattern 2: Plain-text sentence form
#   <do_action> instead of <do_not_action> when <when_condition>.
_SENTENCE_RE = re.compile(
    r"^[-\s\"']*(.+?)\s+instead of\s+(.+?)\s+when\s+(.+?)\.?\s*[\"']*$",
    re.IGNORECASE | re.DOTALL,
)


def _parse_feedback_content(
    text: str,
) -> tuple[str | None, str | None, str | None]:
    """
    Parse feedback_content text into structured fields.

    Tries two formats:
    1. Structured (When/Do/Don't) format
    2. Plain-text sentence ("X instead of Y when Z")

    Args:
        text (str): The feedback_content string to parse

    Returns:
        tuple[str|None, str|None, str|None]: (do_action, do_not_action, when_condition),
            all None if no pattern matches
    """
    # Try structured format first
    m = _STRUCTURED_RE.search(text)
    if m and m.group("when"):
        return (
            _strip_trailing_dot(m.group("do")),
            _strip_trailing_dot(m.group("dont")),
            _strip_trailing_dot(m.group("when")),
        )

    # Try sentence format
    m = _SENTENCE_RE.match(text.strip())
    if m:
        return (
            _strip_trailing_dot(m.group(1)),
            _strip_trailing_dot(m.group(2)),
            _strip_trailing_dot(m.group(3)),
        )

    return None, None, None


def _strip_trailing_dot(s: str | None) -> str | None:
    """Strip trailing period and whitespace from a string, if present."""
    if s is None:
        return None
    return s.strip().rstrip(".")


def _backfill_table(
    cursor: psycopg2.extensions.cursor,
    table: str,
    id_column: str,
) -> tuple[int, int]:
    """
    Backfill do_action, do_not_action, when_condition for one table.

    Args:
        cursor: Database cursor
        table (str): Table name ('raw_feedbacks' or 'feedbacks')
        id_column (str): Primary key column name

    Returns:
        tuple[int, int]: (parsed_count, skipped_count)
    """
    cursor.execute(
        f"SELECT {id_column}, feedback_content FROM {table} "  # noqa: S608
        f"WHERE feedback_content IS NOT NULL "
        f"AND do_action IS NULL AND when_condition IS NULL"
    )
    rows = cursor.fetchall()

    parsed = 0
    skipped = 0

    for row_id, content in rows:
        do_action, do_not_action, when_condition = _parse_feedback_content(content)

        if do_action is not None or when_condition is not None:
            cursor.execute(
                f"UPDATE {table} SET do_action = %s, do_not_action = %s, when_condition = %s "  # noqa: S608
                f"WHERE {id_column} = %s",
                (do_action, do_not_action, when_condition, row_id),
            )
            parsed += 1
        else:
            skipped += 1

    return parsed, skipped


def migrate_20260124120000_structured_feedback_fields(
    conn: psycopg2.extensions.connection,  # noqa: ARG001
    cursor: psycopg2.extensions.cursor,
    schema: str = "public",
) -> None:
    """
    Backfill structured feedback fields (do_action, do_not_action, when_condition)
    by parsing existing feedback_content in raw_feedbacks and feedbacks tables.

    Only updates rows where feedback_content is present but the structured fields
    are NULL. Rows whose content doesn't match known patterns are left unchanged.

    Args:
        conn: Active database connection (do not commit/rollback)
        cursor: Cursor bound to conn
    """
    # execute_migration sets search_path to the target schema before calling
    # data migrations, so unqualified table names below intentionally resolve
    # inside that schema.
    logger.info("Running structured feedback backfill in schema %s", schema)
    for table, id_col in [
        ("raw_feedbacks", "user_playbook_id"),
        ("feedbacks", "feedback_id"),
    ]:
        parsed, skipped = _backfill_table(cursor, table, id_col)
        logger.info(
            "Migration backfill %s: parsed=%d, skipped=%d", table, parsed, skipped
        )


def _reembed_table(
    cursor: psycopg2.extensions.cursor,
    llm_client: LiteLLMClient,
    table: str,
    id_column: str,
    text_builder: Callable,
    dimensions: int,
    embedding_model: str,
    batch_size: int = 100,
) -> int:
    """
    Re-generate embeddings at new dimensions for one table.

    Selects rows where embedding IS NULL, builds text via text_builder,
    generates new embeddings in batches, and updates the rows.

    Args:
        cursor: Database cursor
        llm_client: LiteLLM client for embedding generation
        table (str): Table name
        id_column (str): Primary key column name
        text_builder: Callable that takes a row tuple and returns the text to embed
        dimensions (int): Target embedding dimensions
        embedding_model (str): Embedding model name
        batch_size (int): Number of rows per embedding API call

    Returns:
        int: Number of rows updated
    """
    cursor.execute(f"SELECT * FROM {table} WHERE embedding IS NULL")  # noqa: S608
    columns = [desc[0] for desc in cursor.description]  # type: ignore[reportOptionalIterable]
    rows = cursor.fetchall()

    if not rows:
        logger.info("Re-embed %s: no rows with NULL embedding", table)
        return 0

    # Convert to list of dicts for easier access
    row_dicts = [dict(zip(columns, row, strict=False)) for row in rows]

    updated = 0
    for i in range(0, len(row_dicts), batch_size):
        batch = row_dicts[i : i + batch_size]
        texts = [text_builder(r) for r in batch]
        ids = [r[id_column] for r in batch]

        # Skip rows with empty text
        valid_indices = [j for j, t in enumerate(texts) if t.strip()]
        if not valid_indices:
            continue

        valid_texts = [texts[j] for j in valid_indices]
        valid_ids = [ids[j] for j in valid_indices]

        try:
            embeddings = llm_client.get_embeddings(
                valid_texts, model=embedding_model, dimensions=dimensions
            )
        except EmbeddingUnavailableError as exc:
            logger.warning(
                "Embedding unavailable during re-embed of %s; skipping batch: %s",
                table,
                exc,
            )
            continue

        for row_id, embedding in zip(valid_ids, embeddings, strict=False):
            cursor.execute(
                f"UPDATE {table} SET embedding = %s WHERE {id_column} = %s",  # noqa: S608
                (str(embedding), row_id),
            )
            updated += 1

        logger.info(
            "Re-embed %s: processed batch %d-%d (%d updated)",
            table,
            i,
            i + len(batch),
            len(valid_ids),
        )

    return updated


def migrate_20260202000000_reembed_512(
    conn: psycopg2.extensions.connection,  # noqa: ARG001
    cursor: psycopg2.extensions.cursor,
    schema: str = "public",
) -> None:
    """
    Re-generate embeddings at 512 dimensions for all tables.

    The SQL migration (20260202000000) altered the vector columns to 512 dimensions
    and nulled out existing embeddings. This data migration regenerates them.

    Args:
        conn: Active database connection (do not commit/rollback)
        cursor: Cursor bound to conn
    """
    import os

    from reflexio.models.config_schema import EMBEDDING_DIMENSIONS
    from reflexio.server.llm.litellm_client import LiteLLMClient, LiteLLMConfig

    embedding_model = os.getenv("EMBEDDING_MODEL_NAME", "text-embedding-3-small")
    dimensions = EMBEDDING_DIMENSIONS
    # execute_migration sets search_path to the target schema before calling
    # data migrations, so unqualified table names below intentionally resolve
    # inside that schema.
    logger.info("Running 512-dimension re-embed in schema %s", schema)

    config = LiteLLMConfig(model=embedding_model, temperature=0.0)
    llm_client = LiteLLMClient(config)

    # Table definitions: (table, id_column, text_builder)
    table_configs = [
        (
            "interactions",
            "interaction_id",
            lambda r: "\n".join(
                [r.get("content") or "", r.get("user_action_description") or ""]
            ),
        ),
        (
            "profiles",
            "profile_id",
            lambda r: "\n".join(
                [r.get("profile_content") or "", str(r.get("custom_features") or "")]
            ),
        ),
        (
            "raw_feedbacks",
            "user_playbook_id",
            lambda r: r.get("indexed_content") or r.get("feedback_content") or "",
        ),
        (
            "feedbacks",
            "feedback_id",
            lambda r: r.get("when_condition") or r.get("feedback_content") or "",
        ),
        (
            "agent_success_evaluation_result",
            "result_id",
            lambda r: " ".join(
                filter(
                    None,
                    [
                        r.get("failure_type"),
                        r.get("failure_reason"),
                    ],
                )
            ),
        ),
    ]

    for table, id_col, text_builder in table_configs:
        updated = _reembed_table(
            cursor, llm_client, table, id_col, text_builder, dimensions, embedding_model
        )
        logger.info("Migration re-embed %s: updated=%d rows", table, updated)


DATA_MIGRATIONS: dict[str, DataMigrationFn] = {
    "20260124120000": migrate_20260124120000_structured_feedback_fields,
    "20260202000000": migrate_20260202000000_reembed_512,
}
