-- Durable extraction-agent run tracking for Postgres storage.
--
-- Upstream extraction now records each profile/playbook extraction as an
-- agent run. SQLite already has this table; Postgres needs the same contract
-- for normal launch flows to finalize extraction runs cleanly.

CREATE TABLE IF NOT EXISTS "public"."_agent_runs" (
    id text PRIMARY KEY,
    org_id text NOT NULL,
    extractor_kind text NOT NULL,
    extractor_name text NOT NULL,
    user_id text,
    request_id text NOT NULL,
    agent_version text,
    source text,
    source_interaction_ids integer[] NOT NULL DEFAULT '{}',
    window_start_interaction_id integer,
    window_end_interaction_id integer,
    extractor_config_hash text,
    status text NOT NULL,
    generation_request_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
    service_config_snapshot jsonb,
    agent_context_snapshot text,
    committed_output jsonb,
    pending_tool_call_ids text[] NOT NULL DEFAULT '{}',
    max_steps_remaining integer,
    resume_attempts integer NOT NULL DEFAULT 0,
    finalization_attempts integer NOT NULL DEFAULT 0,
    next_resume_at timestamptz,
    claimed_by text,
    claimed_at timestamptz,
    agent_completed_at timestamptz,
    finalized_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    last_error text
);

CREATE INDEX IF NOT EXISTS "idx_agent_runs_ready"
    ON "public"."_agent_runs" (status, next_resume_at, updated_at);

CREATE INDEX IF NOT EXISTS "idx_agent_runs_binding"
    ON "public"."_agent_runs" (org_id, extractor_kind, extractor_name, user_id);
