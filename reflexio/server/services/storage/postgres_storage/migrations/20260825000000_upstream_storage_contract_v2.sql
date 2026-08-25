-- PostgreSQL parity for the upstream durable finalization, governance lease,
-- and immutable session-outcome contracts.

CREATE TABLE IF NOT EXISTS "public"."_agent_run_finalization_receipts" (
    "run_id" text PRIMARY KEY,
    "entity_type" text NOT NULL
        CHECK ("entity_type" IN ('profile', 'user_playbook')),
    "learning_ids" jsonb NOT NULL,
    "created_at" timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY ("run_id") REFERENCES "public"."_agent_runs" ("id")
        ON DELETE CASCADE
);

ALTER TABLE "public"."purge_operations"
    ADD COLUMN IF NOT EXISTS "authoritative_user_digest" text,
    ADD COLUMN IF NOT EXISTS "execution_claim_owner" text,
    ADD COLUMN IF NOT EXISTS "execution_claim_fence" bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS "execution_claim_expires_at" bigint;

CREATE INDEX IF NOT EXISTS "idx_purge_operations_execution_claim"
    ON "public"."purge_operations" (
        "org_id", "status", "execution_claim_expires_at"
    );

ALTER TABLE "public"."session_outcomes"
    ADD COLUMN IF NOT EXISTS "outcome_id" text,
    ADD COLUMN IF NOT EXISTS "outcome_revision" integer,
    ADD COLUMN IF NOT EXISTS "outcome_contract_digest" text,
    ADD COLUMN IF NOT EXISTS "finalized_trajectory_digest" text;

ALTER TABLE "public"."session_outcomes"
    DROP CONSTRAINT IF EXISTS "session_outcomes_outcome_check";

ALTER TABLE "public"."session_outcomes"
    ADD CONSTRAINT "session_outcomes_outcome_check"
    CHECK ("outcome" IN ('success', 'failure', 'unknown'));

CREATE UNIQUE INDEX IF NOT EXISTS "idx_session_outcomes_outcome_id"
    ON "public"."session_outcomes" ("outcome_id")
    WHERE "outcome_id" IS NOT NULL;

CREATE INDEX IF NOT EXISTS "idx_session_outcomes_occurred_at"
    ON "public"."session_outcomes" ("occurred_at");
