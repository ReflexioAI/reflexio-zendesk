"""Tests for native Postgres migration SQL rendering."""

from reflexio.server.services.storage.postgres_storage._migration_utils import (
    get_latest_migration_version,
    render_migration_sql_for_backend,
)


def test_latest_migration_version_uses_packaged_migrations() -> None:
    assert get_latest_migration_version() == "20260525000000"


def test_postgres_renderer_filters_supabase_only_sql() -> None:
    sql = """
    CREATE EXTENSION IF NOT EXISTS "pg_net" WITH SCHEMA "extensions";
    CREATE EXTENSION IF NOT EXISTS "vector" WITH SCHEMA "public";
    CREATE TABLE IF NOT EXISTS "public"."profiles" (profile_id text PRIMARY KEY);
    GRANT ALL ON TABLE "public"."profiles" TO "authenticated";
    ALTER PUBLICATION "supabase_realtime" OWNER TO "postgres";
    NOTIFY pgrst, 'reload schema';
    """

    rendered = render_migration_sql_for_backend(
        sql, schema="public", target_backend="postgres"
    )

    assert 'CREATE EXTENSION IF NOT EXISTS "vector"' in rendered
    assert 'CREATE TABLE IF NOT EXISTS "public"."profiles"' in rendered
    assert "pg_net" not in rendered
    assert "authenticated" not in rendered
    assert "supabase_realtime" not in rendered
    assert "NOTIFY pgrst" not in rendered


def test_postgres_renderer_rewrites_app_public_objects_for_schema() -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS "public"."profiles" (profile_id text PRIMARY KEY);
    CREATE EXTENSION IF NOT EXISTS "vector" WITH SCHEMA "public";
    """

    rendered = render_migration_sql_for_backend(
        sql, schema="org_test", target_backend="postgres"
    )

    assert 'CREATE TABLE IF NOT EXISTS "org_test"."profiles"' in rendered
    assert 'CREATE EXTENSION IF NOT EXISTS "vector" WITH SCHEMA "public"' in rendered
