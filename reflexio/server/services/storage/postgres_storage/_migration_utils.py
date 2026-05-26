"""
Utility functions for Postgres storage operations.

Shared helpers (timestamp parsing, DB/migration utilities, org config) live here.
Request converters also remain here as they are small cross-cutting groups.

Profile/interaction converters: see ``_profile_converters``
Playbook/evaluation converters: see ``_playbook_converters``
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import psycopg2
from psycopg2 import sql as psycopg2_sql

from reflexio.models.api_schema.service_schemas import (
    Request,
)

# ---------------------------------------------------------------------------
# Re-export playbook converters for backward compatibility
# ---------------------------------------------------------------------------
from reflexio.server.services.storage.postgres_storage._playbook_converters import (
    agent_playbook_to_data,
    agent_success_evaluation_result_to_data,
    playbook_aggregation_change_log_to_data,
    response_list_to_playbook_aggregation_change_logs,
    response_to_playbook_aggregation_change_log,
    user_playbook_to_data,
)

# ---------------------------------------------------------------------------
# Re-export profile converters
# ---------------------------------------------------------------------------
from reflexio.server.services.storage.postgres_storage._profile_converters import (
    interaction_to_data,
    profile_change_log_to_data,
    response_list_to_interactions,
    response_list_to_profile_change_logs,
    response_list_to_user_profiles,
    response_to_interaction,
    response_to_profile_change_log,
    response_to_user_profile,
    user_profile_to_data,
)

logger = logging.getLogger(__name__)

Client = Any
_MIGRATION_DIR = Path(__file__).resolve().parent / "migrations"

_APP_PUBLIC_OBJECTS = {
    "_operation_state",
    "agent_playbooks",
    "agent_success_evaluation_result",
    "agent_success_evaluation_result_result_id_seq",
    "feedback_aggregation_change_logs_id_seq",
    "feedbacks_feedback_id_seq",
    "get_last_k_interactions",
    "get_new_request_interaction_groups",
    "hybrid_match_agent_playbooks",
    "hybrid_match_interactions",
    "hybrid_match_profiles",
    "hybrid_match_skills",
    "hybrid_match_user_playbooks",
    "interactions",
    "interactions_interaction_id_seq",
    "json_values_as_text",
    "match_interactions",
    "match_profiles",
    "playbook_aggregation_change_logs",
    "profile_change_logs",
    "profile_change_logs_id_seq",
    "profiles",
    "raw_feedbacks_raw_feedback_id_seq",
    "requests",
    "share_links",
    "share_links_id_seq",
    "skills",
    "skills_skill_id_seq",
    "try_acquire_in_progress_lock",
    "user_playbooks",
}

# ---------------------------------------------------------------------------
# Make re-exports visible to static analysis and wildcard imports
# ---------------------------------------------------------------------------
__all__ = [
    # profile converters
    "interaction_to_data",
    "profile_change_log_to_data",
    "response_list_to_interactions",
    "response_list_to_profile_change_logs",
    "response_list_to_user_profiles",
    "response_to_interaction",
    "response_to_profile_change_log",
    "response_to_user_profile",
    "user_profile_to_data",
    # playbook converters
    "agent_playbook_to_data",
    "agent_success_evaluation_result_to_data",
    "playbook_aggregation_change_log_to_data",
    "response_list_to_playbook_aggregation_change_logs",
    "response_to_playbook_aggregation_change_log",
    "user_playbook_to_data",
    # request converters
    "request_to_data",
    "response_list_to_requests",
    "response_to_request",
    # shared helpers
    "_parse_iso_timestamp",
    "check_migration_needed",
    "add_schema_to_postgrest",
    "drop_schema",
    "execute_migration",
    "execute_postgres_prerequisites",
    "execute_sql_file_direct",
    "extract_db_url_and_schema_from_config_json",
    "extract_db_url_from_config_json",
    "get_latest_migration_version",
    "get_organization_config",
    "get_organization_config_version",
    "is_localhost_url",
    "render_migration_sql_for_schema",
    "render_migration_sql_for_backend",
    "remove_schema_from_postgrest",
    "set_organization_config",
    "split_sql_statements",
    "wait_for_schema_ready",
]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


from reflexio.server.services.storage.postgres_storage._timestamp_utils import (  # noqa: E402
    _parse_iso_timestamp,
)

_parse_iso_timestamp = _parse_iso_timestamp  # re-export for backward compatibility


# ---------------------------------------------------------------------------
# Request converters
# ---------------------------------------------------------------------------


def response_to_request(item: Mapping[str, Any]) -> Request:
    """
    Convert a response item from Supabase to a Request object.

    Args:
        item: Dictionary containing request data from Supabase response

    Returns:
        Request object
    """
    return Request(
        request_id=item["request_id"],
        user_id=item["user_id"],
        created_at=_parse_iso_timestamp(item["created_at"]),
        source=item.get("source", ""),
        agent_version=item.get("agent_version", ""),
        session_id=item.get("session_id"),
    )


def request_to_data(request: Request) -> dict[str, Any]:
    """
    Convert a Request object to data for upserting into Supabase.

    Args:
        request: Request object to convert

    Returns:
        Dictionary containing data ready for upsert
    """
    return {
        "request_id": request.request_id,
        "user_id": request.user_id,
        "created_at": datetime.fromtimestamp(request.created_at, tz=UTC).isoformat(),
        "source": request.source,
        "agent_version": request.agent_version,
        "session_id": request.session_id or None,
    }


def response_list_to_requests(response_data: list[dict[str, Any]]) -> list[Request]:
    """
    Convert a list of response items to Request objects.

    Args:
        response_data: List of dictionaries containing request data from Supabase response

    Returns:
        List of Request objects
    """
    return [response_to_request(item) for item in response_data]


# ---------------------------------------------------------------------------
# Database / migration utilities
# ---------------------------------------------------------------------------


def is_localhost_url(db_url: str) -> bool:
    """
    Check if the database URL points to localhost.

    Args:
        db_url: Database connection URL

    Returns:
        bool: True if the URL is localhost, False otherwise
    """
    try:
        parsed = urlparse(db_url)
        host = parsed.hostname or ""
        return host in ("localhost", "127.0.0.1", "::1")
    except (ValueError, AttributeError):  # fmt: skip
        return False


def get_latest_migration_version() -> str | None:
    """
    Get the version prefix of the latest migration file on disk.

    Returns:
        str | None: The version string of the latest migration, or None if no migrations found
    """
    migration_files = sorted(_MIGRATION_DIR.glob("*.sql"))
    if not migration_files:
        return None

    filename = migration_files[-1].name
    return filename.split("_")[0]


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _schema_tracking_table(schema: str) -> tuple[str, str]:
    if schema == "public":
        return "supabase_migrations", "schema_migrations"
    return schema, "_schema_migrations"


def _extract_created_public_objects(migration_sql: str) -> set[str]:
    objects: set[str] = set()
    object_pattern = re.compile(
        r"""
        \bCREATE\s+
        (?:OR\s+REPLACE\s+)?
        (?:TABLE|FUNCTION|SEQUENCE|VIEW|MATERIALIZED\s+VIEW|TYPE)
        \s+(?:IF\s+NOT\s+EXISTS\s+)?
        (?:
            "public"\."(?P<quoted>[A-Za-z_][A-Za-z0-9_]*)"
            |
            public\.(?P<bare>[A-Za-z_][A-Za-z0-9_]*)
        )
        """,
        flags=re.IGNORECASE | re.VERBOSE,
    )
    index_pattern = re.compile(
        r"""
        \bCREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?
        (?:"public"\.)?"(?P<quoted_index>[A-Za-z_][A-Za-z0-9_]*)"|
        \bCREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?
        (?:public\.)?(?P<bare_index>[A-Za-z_][A-Za-z0-9_]*)
        \s+ON\s+
        (?:
            "public"\."(?P<quoted_table>[A-Za-z_][A-Za-z0-9_]*)"
            |
            public\.(?P<bare_table>[A-Za-z_][A-Za-z0-9_]*)
        )
        """,
        flags=re.IGNORECASE | re.VERBOSE,
    )

    for match in object_pattern.finditer(migration_sql):
        objects.add(match.group("quoted") or match.group("bare"))
    for match in index_pattern.finditer(migration_sql):
        for group_name in (
            "quoted_index",
            "bare_index",
            "quoted_table",
            "bare_table",
        ):
            value = match.group(group_name)
            if value:
                objects.add(value)
    return objects


def _migration_public_objects() -> set[str]:
    objects = set(_APP_PUBLIC_OBJECTS)
    for migration_file in _MIGRATION_DIR.glob("*.sql"):
        try:
            objects.update(
                _extract_created_public_objects(
                    migration_file.read_text(encoding="utf-8")
                )
            )
        except OSError:
            logger.debug("Failed to read migration file %s", migration_file)
    return objects


def _parse_postgrest_schema_setting(setting: str | None) -> list[str]:
    if not setting:
        return []
    value = setting
    if value.startswith("pgrst.db_schemas="):
        value = value.removeprefix("pgrst.db_schemas=")
    return [item.strip() for item in value.split(",") if item.strip()]


def _read_persisted_postgrest_schemas(cursor: Any) -> list[str]:
    cursor.execute(
        """
        SELECT cfg
        FROM pg_db_role_setting s
        JOIN pg_roles r ON r.oid = s.setrole
        JOIN pg_database d ON d.oid = s.setdatabase
        CROSS JOIN LATERAL unnest(s.setconfig) AS cfg
        WHERE r.rolname = 'authenticator'
          AND d.datname = current_database()
          AND cfg LIKE 'pgrst.db_schemas=%'
        LIMIT 1
        """
    )
    row = cursor.fetchone()
    return _parse_postgrest_schema_setting(row[0] if row and row[0] else None)


def _write_postgrest_schemas(cursor: Any, schemas: list[str]) -> None:
    cursor.execute("SELECT current_database()")
    database_row = cursor.fetchone()
    database_name = database_row[0] if database_row else "postgres"
    cursor.execute(
        psycopg2_sql.SQL(
            "ALTER ROLE authenticator IN DATABASE {} SET pgrst.db_schemas = %s"
        ).format(psycopg2_sql.Identifier(database_name)),
        (",".join(schemas),),
    )
    cursor.execute("NOTIFY pgrst, 'reload config'")
    cursor.execute("NOTIFY pgrst, 'reload schema'")


def render_migration_sql_for_schema(migration_sql: str, schema: str) -> str:
    """Render a data migration SQL file for a target app schema.

    ``public`` is the historical default and returns the SQL unchanged. For
    per-org schemas, Reflexio-owned ``public`` object qualifiers are moved into
    the target schema while extension/type references such as ``public.vector``
    stay in ``public``.
    """
    if schema == "public":
        return migration_sql

    quoted_schema = _quote_identifier(schema)
    app_public_objects = _migration_public_objects()

    def replace_quoted_public(match: re.Match[str]) -> str:
        object_name = match.group(1)
        if object_name in app_public_objects:
            return f'{quoted_schema}."{object_name}"'
        return match.group(0)

    def replace_bare_public(match: re.Match[str]) -> str:
        object_name = match.group(1)
        if object_name in app_public_objects:
            return f"{quoted_schema}.{object_name}"
        return match.group(0)

    rendered = re.sub(
        r'"public"\."([A-Za-z_][A-Za-z0-9_]*)"',
        replace_quoted_public,
        migration_sql,
    )
    rendered = re.sub(
        r"\bpublic\.([A-Za-z_][A-Za-z0-9_]*)",
        replace_bare_public,
        rendered,
    )
    return re.sub(
        r"(table_schema\s*=\s*)'public'",
        rf"\1{_quote_literal(schema)}",
        rendered,
        flags=re.IGNORECASE,
    )


def check_migration_needed(db_url: str, schema: str = "public") -> bool:
    """
    Quick check whether the latest migration has been applied to the given database.

    Connects with a short timeout, queries the schema_migrations table, and returns
    True if the latest migration version is NOT present (i.e. migration is needed).
    Returns False on any error (fail-safe: skip migration on uncertainty).

    Args:
        db_url: PostgreSQL connection string

    Returns:
        bool: True if migration is needed, False if up-to-date or on error
    """
    latest_version = get_latest_migration_version()
    if not latest_version:
        return False

    conn = None
    try:
        conn = psycopg2.connect(db_url, connect_timeout=5)
        cursor = conn.cursor()
        tracking_schema, tracking_table = _schema_tracking_table(schema)
        cursor.execute(
            psycopg2_sql.SQL("SELECT 1 FROM {}.{} WHERE version = %s").format(
                psycopg2_sql.Identifier(tracking_schema),
                psycopg2_sql.Identifier(tracking_table),
            ),
            (latest_version,),
        )
        row = cursor.fetchone()
        cursor.close()
        return row is None
    except Exception as e:
        logger.debug("check_migration_needed failed for %s: %s", db_url, e)
        return False
    finally:
        if conn is not None:
            conn.close()


def split_sql_statements(sql_text: str) -> list[str]:  # noqa: C901
    """Split SQL text into statements while preserving quoted function bodies."""
    statements: list[str] = []
    current: list[str] = []
    i = 0
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    dollar_tag: str | None = None

    while i < len(sql_text):
        ch = sql_text[i]
        next_ch = sql_text[i + 1] if i + 1 < len(sql_text) else ""
        if in_line_comment:
            current.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            current.append(ch)
            if ch == "*" and next_ch == "/":
                current.append(next_ch)
                i += 2
                in_block_comment = False
                continue
            i += 1
            continue

        if dollar_tag is not None:
            if sql_text.startswith(dollar_tag, i):
                current.append(dollar_tag)
                i += len(dollar_tag)
                dollar_tag = None
                continue
            current.append(ch)
            i += 1
            continue

        if in_single:
            current.append(ch)
            if ch == "'" and i + 1 < len(sql_text) and sql_text[i + 1] == "'":
                current.append(sql_text[i + 1])
                i += 2
                continue
            if ch == "'":
                in_single = False
            i += 1
            continue

        if in_double:
            current.append(ch)
            if ch == '"':
                in_double = False
            i += 1
            continue

        if ch == "'":
            in_single = True
            current.append(ch)
            i += 1
            continue
        if ch == '"':
            in_double = True
            current.append(ch)
            i += 1
            continue
        if ch == "-" and next_ch == "-":
            in_line_comment = True
            current.append(ch)
            current.append(next_ch)
            i += 2
            continue
        if ch == "/" and next_ch == "*":
            in_block_comment = True
            current.append(ch)
            current.append(next_ch)
            i += 2
            continue
        if ch == "$":
            match = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql_text[i:])
            if match:
                dollar_tag = match.group(0)
                current.append(dollar_tag)
                i += len(dollar_tag)
                continue
        if ch == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            i += 1
            continue
        current.append(ch)
        i += 1

    statement = "".join(current).strip()
    if statement:
        statements.append(statement)
    return statements


_POSTGRES_SKIPPED_EXTENSIONS = {
    "pg_net",
    "pg_graphql",
    "pg_stat_statements",
    "pgcrypto",
    "supabase_vault",
    "uuid-ossp",
}


_POSTGRES_PREREQUISITES_SQL = """
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE SCHEMA IF NOT EXISTS public;
COMMENT ON SCHEMA public IS 'standard public schema';
"""


def execute_postgres_prerequisites(db_url: str) -> tuple[bool, str]:
    """Prepare database-level prerequisites for native Postgres storage."""
    try:
        with (
            closing(psycopg2.connect(db_url)) as conn,
            closing(conn.cursor()) as cursor,
        ):
            cursor.execute(_POSTGRES_PREREQUISITES_SQL)
            conn.commit()
        return True, "Postgres prerequisites are ready"
    except Exception as e:
        return False, str(e)


def render_migration_sql_for_backend(
    migration_sql: str,
    schema: str,
    target_backend: Literal["supabase", "postgres"] = "supabase",
) -> str:
    """Render migration SQL for a target schema/backend pair."""
    rendered_sql = render_migration_sql_for_schema(migration_sql, schema)
    if target_backend == "supabase":
        return rendered_sql
    statements = [
        statement
        for statement in split_sql_statements(rendered_sql)
        if not _should_skip_postgres_statement(statement)
    ]
    return ";\n\n".join(statements) + (";\n" if statements else "")


def _should_skip_postgres_statement(statement: str) -> bool:
    normalized = " ".join(statement.strip().split())
    upper = normalized.upper()
    if upper.startswith("NOTIFY PGRST"):
        return True
    if re.match(
        r'^ALTER PUBLICATION "?supabase_realtime"? ', normalized, re.IGNORECASE
    ):
        return True
    if re.search(
        r'\bTO\s+"?(anon|authenticated|service_role|postgres)"?\s*$',
        normalized,
        re.IGNORECASE,
    ):
        return True
    if re.search(r'\bOWNER\s+TO\s+"?postgres"?\s*$', normalized, re.IGNORECASE):
        return True
    extension_match = re.match(
        r'^CREATE EXTENSION IF NOT EXISTS "?([^"\s]+)"?', normalized, re.IGNORECASE
    )
    return bool(
        extension_match and extension_match.group(1) in _POSTGRES_SKIPPED_EXTENSIONS
    )


def _run_postgres_schema_smoke_check(cursor: Any, schema: str) -> None:
    expected_tables = ["profiles", "interactions", "requests", "user_playbooks"]
    expected_functions = [
        "hybrid_match_profiles",
        "hybrid_match_interactions",
        "hybrid_match_user_playbooks",
        "try_acquire_in_progress_lock",
    ]
    cursor.execute(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = ANY(%s)
        """,
        (schema, expected_tables),
    )
    found_tables = {row[0] for row in cursor.fetchall()}
    missing_tables = sorted(set(expected_tables) - found_tables)
    cursor.execute(
        """
        SELECT p.proname
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = %s AND p.proname = ANY(%s)
        """,
        (schema, expected_functions),
    )
    found_functions = {row[0] for row in cursor.fetchall()}
    missing_functions = sorted(set(expected_functions) - found_functions)
    if missing_tables or missing_functions:
        raise RuntimeError(
            "Postgres migration smoke check failed: "
            f"missing_tables={missing_tables}, missing_functions={missing_functions}"
        )


def extract_db_url_from_config_json(config_json_str: str) -> str | None:
    """
    Parse a (already-decrypted) config JSON string and extract the database URL.

    Args:
        config_json_str: Decrypted JSON configuration string

    Returns:
        str | None: The db_url from storage_config, or None if not found
    """
    try:
        config_data = json.loads(str(config_json_str))
        storage_config = config_data.get("storage_config")
        if storage_config and "db_url" in storage_config:
            db_url = storage_config["db_url"]
            return db_url or None
        return None
    except Exception as e:
        logger.debug("extract_db_url_from_config_json failed: %s", e)
        return None


def extract_db_url_and_schema_from_config_json(
    config_json_str: str,
) -> tuple[str | None, str | None]:
    """Extract the storage db_url and optional schema from decrypted config JSON."""
    try:
        config_data = json.loads(str(config_json_str))
        storage_config = config_data.get("storage_config")
        if not isinstance(storage_config, dict):
            return None, None
        db_url = storage_config.get("db_url") or None
        schema = storage_config.get("schema") or storage_config.get("schema_name")
        return db_url, schema or None
    except Exception as e:
        logger.debug("extract_db_url_and_schema_from_config_json failed: %s", e)
        return None, None


def execute_sql_file_direct(db_url: str, file_path: str) -> list[Any]:
    """
    Execute SQL file using direct database connection.
    Requires database URL with proper credentials.

    Args:
        db_url: PostgreSQL connection string
        file_path: Path to the SQL file

    Returns:
        List of results from executed statements
    """
    if not db_url:
        raise ValueError("Database URL is required for direct execution")

    try:
        # Connect directly to PostgreSQL
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()

        # Read and execute SQL file
        with Path(file_path).open(encoding="utf-8") as file:
            sql_content = file.read()

        # Execute the SQL (split by semicolons for multiple statements)
        statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

        results = []
        for statement in statements:
            cursor.execute(statement)
            try:
                # Try to fetch results (for SELECT statements)
                result = cursor.fetchall()
                results.append(result)
            except psycopg2.ProgrammingError:
                # No results to fetch (INSERT, UPDATE, DELETE, etc.)
                results.append(f"Executed: {statement[:50]}...")

        conn.commit()
        cursor.close()
        conn.close()

        return results

    except Exception as e:
        print(f"Error executing SQL file: {e}")
        if "conn" in locals():
            conn.rollback()
            conn.close()
        raise e


def execute_migration(
    db_url: str,
    schema: str = "public",
    target_backend: Literal["supabase", "postgres"] = "supabase",
) -> tuple[bool, str]:
    """
    This routine pushes the current migration onto the remote db.

    Args:
        db_url (str): PostgreSQL connection string (use pooler URL with port 6543 for IPv4 support)
        schema: Target application schema. ``public`` preserves legacy behavior.
        target_backend: SQL target backend. ``supabase`` preserves PostgREST-specific
            statements and schema reloads; ``postgres`` filters Supabase-only SQL.

    Returns:
        tuple[bool, str]: (success, message)
    """
    from reflexio.server.services.storage.postgres_storage._data_migrations import (
        DATA_MIGRATIONS,
    )

    migration_files = sorted(_MIGRATION_DIR.glob("*.sql"))
    if not migration_files:
        return False, "No migration files found"

    try:
        with (
            closing(psycopg2.connect(db_url)) as conn,
            closing(conn.cursor()) as cursor,
        ):
            tracking_schema, tracking_table = _schema_tracking_table(schema)
            if schema == "public":
                cursor.execute("CREATE SCHEMA IF NOT EXISTS supabase_migrations;")
            else:
                cursor.execute(
                    psycopg2_sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        psycopg2_sql.Identifier(schema)
                    )
                )
                if target_backend == "supabase":
                    cursor.execute(
                        psycopg2_sql.SQL(
                            "GRANT USAGE ON SCHEMA {} TO anon, authenticated, service_role"
                        ).format(psycopg2_sql.Identifier(schema))
                    )
            cursor.execute(
                psycopg2_sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.{} (
                        version text PRIMARY KEY,
                        statements text[],
                        name text,
                        applied_at timestamptz DEFAULT now()
                    );
                    """
                ).format(
                    psycopg2_sql.Identifier(tracking_schema),
                    psycopg2_sql.Identifier(tracking_table),
                )
            )

            executed_migrations = []
            # Capture a failure message instead of returning early; we need
            # to fall through and trigger the PostgREST schema-reload NOTIFY
            # for any earlier migrations that committed before the failure.
            failure_message: str | None = None

            for migration_file in migration_files:
                filename = Path(migration_file).name
                version = filename.split("_")[0]

                cursor.execute(
                    psycopg2_sql.SQL(
                        "SELECT version FROM {}.{} WHERE version = %s"
                    ).format(
                        psycopg2_sql.Identifier(tracking_schema),
                        psycopg2_sql.Identifier(tracking_table),
                    ),
                    (version,),
                )

                if cursor.fetchone() is not None:
                    continue

                try:
                    migration_sql = Path(migration_file).read_text(encoding="utf-8")
                except UnicodeDecodeError as e:
                    failure_message = f"Failed to decode {filename}: {e}"
                    break

                try:
                    rendered_sql = render_migration_sql_for_backend(
                        migration_sql, schema, target_backend
                    )
                    cursor.execute(rendered_sql)

                    if version in DATA_MIGRATIONS:
                        cursor.execute(
                            psycopg2_sql.SQL(
                                "SET LOCAL search_path TO {}, public, extensions"
                            ).format(psycopg2_sql.Identifier(schema))
                        )
                        DATA_MIGRATIONS[version](conn, cursor, schema)

                    executed_migrations.append(filename)
                except Exception as e:
                    conn.rollback()
                    error_str = str(e)
                    # Schema may already exist (applied manually or via Supabase
                    # CLI) but not tracked in schema_migrations. This case only
                    # arises for the legacy ``public`` flow where the database
                    # might have been seeded externally; for per-org schemas we
                    # always provision from a clean slate, so the same error
                    # signals a real failure (e.g., a CREATE OR REPLACE
                    # FUNCTION whose new return type doesn't match the existing
                    # one) and must NOT be silently recorded as applied —
                    # otherwise downstream DDL like ``ADD COLUMN trigger`` is
                    # silently dropped on rollback.
                    schema_conflict = (
                        target_backend == "supabase"
                        and schema == "public"
                        and (
                            "already exists" in error_str
                            or "cannot change return type" in error_str
                        )
                    )
                    if schema_conflict:
                        logger.info(
                            "Migration %s: schema already exists, recording as applied",
                            filename,
                        )
                    else:
                        failure_message = f"Failed to execute {filename}: {error_str}"
                        break

                statements = [s.strip() for s in rendered_sql.split(";") if s.strip()]

                cursor.execute(
                    psycopg2_sql.SQL(
                        "INSERT INTO {}.{} (version, statements, name) VALUES (%s, %s, %s)"
                    ).format(
                        psycopg2_sql.Identifier(tracking_schema),
                        psycopg2_sql.Identifier(tracking_table),
                    ),
                    (version, statements, filename),
                )
                # Commit after each migration so that schema_migrations rows
                # for already-applied migrations survive if a later migration
                # fails. DDL in Postgres auto-commits at the transaction level,
                # but the schema_migrations INSERT is still open until here.
                conn.commit()

            if target_backend == "postgres" and failure_message is None:
                _run_postgres_schema_smoke_check(cursor, schema)
                conn.commit()

            # Always tell PostgREST to reload its schema cache. We do this
            # unconditionally — even when no new migration was applied — because
            # PostgREST caches the schema in-memory and has no way to detect
            # tables/columns added via direct psycopg2 execution (which bypasses
            # the PostgreSQL event triggers Supabase uses for auto-reload on
            # Dashboard/CLI migrations). Without this NOTIFY, a new table is
            # invisible to the Supabase client with "Could not find the table
            # 'public.<name>' in the schema cache" until PostgREST is restarted.
            if target_backend == "supabase":
                cursor.execute("NOTIFY pgrst, 'reload schema'")
                conn.commit()

        # Surface mid-loop failure AFTER the schema reload above ran for any
        # earlier migrations that committed successfully. Otherwise an early
        # return would leave PostgREST's cache stale relative to the rows
        # already in supabase_migrations.
        if failure_message is not None:
            return False, failure_message

        if executed_migrations:
            return True, f"Executed migrations: {', '.join(executed_migrations)}"
        if target_backend == "supabase":
            return True, "All migrations already applied (PostgREST cache refreshed)"
        return True, "All migrations already applied"

    except psycopg2.OperationalError as e:
        error_msg = str(e)
        if (
            "could not translate host name" in error_msg
            or "Name or service not known" in error_msg
        ):
            return (
                False,
                f"DNS resolution failed. Try using the pooler URL (port 6543) for IPv4 support. Error: {error_msg}",
            )
        if "connection refused" in error_msg.lower():
            return (
                False,
                f"Connection refused. Check if your IP is allowed in Supabase network settings. Error: {error_msg}",
            )
        return False, f"Database connection error: {error_msg}"
    except Exception as e:
        return False, str(e)


def add_schema_to_postgrest(db_url: str, schema: str) -> None:
    """Append a schema to PostgREST's exposed schema list safely."""
    with (
        closing(psycopg2.connect(db_url)) as conn,
        closing(conn.cursor()) as cursor,
    ):
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext('pgrst.db_schemas'))")
        schemas = _read_persisted_postgrest_schemas(cursor)
        if not schemas:
            schemas = ["public", "graphql_public"]
        if schema not in schemas:
            schemas.append(schema)

        _write_postgrest_schemas(cursor, schemas)
        conn.commit()


def remove_schema_from_postgrest(db_url: str, schema: str) -> None:
    """Remove a schema from PostgREST's exposed schema list if present."""
    with (
        closing(psycopg2.connect(db_url)) as conn,
        closing(conn.cursor()) as cursor,
    ):
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext('pgrst.db_schemas'))")
        schemas = _read_persisted_postgrest_schemas(cursor)
        if schema not in schemas:
            conn.commit()
            return

        updated_schemas = [item for item in schemas if item != schema]
        if not updated_schemas:
            updated_schemas = ["public", "graphql_public"]
        _write_postgrest_schemas(cursor, updated_schemas)
        conn.commit()


def wait_for_schema_ready(
    supabase_url: str,
    supabase_key: str,
    schema: str,
    timeout_seconds: float = 5.0,
    interval_seconds: float = 0.25,
) -> bool:
    """PostgREST readiness is not used by native Postgres storage."""
    del supabase_url, supabase_key, schema, timeout_seconds, interval_seconds
    return False


def drop_schema(db_url: str, schema: str) -> None:
    """Drop a provisioned schema and all contained objects."""
    if schema == "public":
        raise ValueError("Refusing to drop public schema")
    with (
        closing(psycopg2.connect(db_url)) as conn,
        closing(conn.cursor()) as cursor,
    ):
        cursor.execute(
            psycopg2_sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                psycopg2_sql.Identifier(schema)
            )
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Organization config utilities
# ---------------------------------------------------------------------------


def get_organization_config(client: Client, org_id: str) -> str | None:
    """
    Get the configuration_json for an organization from Supabase.

    Args:
        client: Supabase client
        org_id: Organization ID

    Returns:
        str | None: The encrypted configuration JSON string, or None if not found
    """
    response = (
        client.table("organizations")
        .select("configuration_json")
        .eq("id", org_id)
        .execute()
    )

    if not response.data:
        return None

    row = response.data[0]
    if not isinstance(row, dict):
        return None

    value = row.get("configuration_json")
    return str(value) if value is not None else None


def get_organization_config_version(client: Client, org_id: str) -> int | None:
    """Get the current config_version for an organization from Supabase.

    Used by the Reflexio cache as a cheap probe (single-column SELECT)
    to detect that another replica or an admin SQL update bumped the
    config without going through ``invalidate_reflexio_cache``.

    Args:
        client: Supabase client.
        org_id: Organization ID.

    Returns:
        int | None: The current ``config_version`` on the row, or
        ``None`` if the org isn't found / the column wasn't returned.
    """
    try:
        response = (
            client.table("organizations")
            .select("config_version")
            .eq("id", org_id)
            .execute()
        )
    except Exception as exc:
        if _is_missing_config_version_error(exc):
            return None
        raise
    if not response.data:
        return None
    row = response.data[0]
    if not isinstance(row, dict):
        return None
    value = row.get("config_version")
    if value is None or isinstance(value, (dict, list)):
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _is_missing_config_version_error(exc: Exception) -> bool:
    """Return True when PostgREST reports an older organizations schema."""
    message = str(exc).lower()
    return "config_version" in message and (
        "schema cache" in message
        or "could not find" in message
        or "does not exist" in message
    )


def set_organization_config(client: Client, org_id: str, config_json: str) -> bool:
    """
    Set the configuration_json for an organization in Supabase.

    Bumps ``config_version`` by 1 in the same UPDATE so the Reflexio
    cache (which probes that column) sees a fresh value on its next
    hit. The two columns must move together — splitting the UPDATE
    into two statements would expose a window where another reader
    sees the new config but the old version, and skips its eviction.

    Args:
        client: Supabase client
        org_id: Organization ID
        config_json: The encrypted configuration JSON string

    Returns:
        bool: True if successful, False otherwise
    """
    # First check if org exists AND grab the current config_version for
    # the atomic bump. PostgREST does not expose `column = column + 1`
    # directly, so we read-modify-write inside a single update payload —
    # in practice the read happens here and the write below; if a
    # concurrent writer bumps between them, both updates still result
    # in a fresh version (just not strictly +1).
    version_supported = True
    try:
        response = (
            client.table("organizations")
            .select("id,config_version")
            .eq("id", org_id)
            .execute()
        )
    except Exception as exc:
        if not _is_missing_config_version_error(exc):
            raise
        version_supported = False
        response = client.table("organizations").select("id").eq("id", org_id).execute()

    if not response.data:
        return False

    row = response.data[0]
    current_version = 0
    if isinstance(row, dict):
        raw_version = row.get("config_version")
        if raw_version is not None and not isinstance(raw_version, (dict, list)):
            try:
                current_version = int(raw_version)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                current_version = 0
    payload: dict[str, Any] = {"configuration_json": config_json}
    if version_supported:
        payload["config_version"] = current_version + 1

    try:
        client.table("organizations").update(payload).eq("id", org_id).execute()
    except Exception as exc:
        if version_supported and _is_missing_config_version_error(exc):
            retry_payload = dict(payload)
            retry_payload.pop("config_version", None)
            client.table("organizations").update(retry_payload).eq(
                "id", org_id
            ).execute()
        else:
            raise

    return True
