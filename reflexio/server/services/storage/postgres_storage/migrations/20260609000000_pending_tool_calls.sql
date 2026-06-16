-- Durable pending human-tool-call state for resumable extraction.
--
-- SQLite added these tables with the upstream resumable-agent flow. Native
-- Postgres storage needs the same contract so deployments can pause on
-- follow-up questions, resume when answers arrive, and expire stale prompts.

CREATE TABLE IF NOT EXISTS "public"."_pending_tool_calls" (
    id text PRIMARY KEY,
    org_id text NOT NULL,
    user_id text,
    scope jsonb NOT NULL DEFAULT '{}'::jsonb,
    scope_hash text NOT NULL,
    tool_name text NOT NULL,
    dedup_key text NOT NULL,
    status text NOT NULL,
    question_text text NOT NULL,
    answer_format text,
    args jsonb NOT NULL DEFAULT '{}'::jsonb,
    tags jsonb NOT NULL DEFAULT '[]'::jsonb,
    result jsonb,
    embedding jsonb,
    superseded_by text,
    created_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    expires_at timestamptz NOT NULL,
    cache_until timestamptz NOT NULL,
    valid_until timestamptz
);

CREATE INDEX IF NOT EXISTS "idx_pending_tool_calls_active"
    ON "public"."_pending_tool_calls" (
        org_id, scope_hash, tool_name, dedup_key, status, cache_until
    );

CREATE INDEX IF NOT EXISTS "idx_pending_tool_calls_prior"
    ON "public"."_pending_tool_calls" (
        org_id, scope_hash, tool_name, status, valid_until
    );

CREATE TABLE IF NOT EXISTS "public"."_run_tool_dependencies" (
    run_id text NOT NULL,
    pending_tool_call_id text NOT NULL,
    dependency_kind text NOT NULL DEFAULT 'followup',
    resolved_at timestamptz,
    consumed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, pending_tool_call_id),
    FOREIGN KEY (run_id) REFERENCES "public"."_agent_runs"(id) ON DELETE CASCADE,
    FOREIGN KEY (pending_tool_call_id)
        REFERENCES "public"."_pending_tool_calls"(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS "idx_run_tool_dependencies_pending"
    ON "public"."_run_tool_dependencies" (
        pending_tool_call_id, resolved_at, consumed_at
    );

CREATE INDEX IF NOT EXISTS "idx_run_tool_dependencies_ready"
    ON "public"."_run_tool_dependencies" (
        run_id, resolved_at, consumed_at
    );
