-- Storage contract additions merged from upstream for native PostgreSQL.

ALTER TABLE "public"."requests"
    ADD COLUMN IF NOT EXISTS "evaluation_only" boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS "retrieval_experiment_id" text,
    ADD COLUMN IF NOT EXISTS "retrieval_experiment_arm" text,
    ADD COLUMN IF NOT EXISTS "governance_subject_ref" text;

ALTER TABLE "public"."interactions"
    ADD COLUMN IF NOT EXISTS "token_count" integer,
    ADD COLUMN IF NOT EXISTS "retrieved_learnings" jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS "governance_subject_ref" text;

ALTER TABLE "public"."profiles"
    ADD COLUMN IF NOT EXISTS "governance_subject_ref" text;

ALTER TABLE "public"."user_playbooks"
    ADD COLUMN IF NOT EXISTS "governance_subject_ref" text;

ALTER TABLE "public"."agent_success_evaluation_result"
    ADD COLUMN IF NOT EXISTS "user_id" text,
    ADD COLUMN IF NOT EXISTS "tags" jsonb,
    ADD COLUMN IF NOT EXISTS "governance_subject_ref" text;

ALTER TABLE "public"."lineage_event"
    ADD COLUMN IF NOT EXISTS "model_name" text,
    ADD COLUMN IF NOT EXISTS "provider" text;

CREATE INDEX IF NOT EXISTS "idx_requests_retrieval_experiment"
    ON "public"."requests" (
        "retrieval_experiment_id", "user_id", "session_id", "created_at", "request_id"
    );

CREATE INDEX IF NOT EXISTS "idx_requests_governance_subject_ref"
    ON "public"."requests" ("governance_subject_ref");
CREATE INDEX IF NOT EXISTS "idx_interactions_governance_subject_ref"
    ON "public"."interactions" ("governance_subject_ref");
CREATE INDEX IF NOT EXISTS "idx_profiles_governance_subject_ref"
    ON "public"."profiles" ("governance_subject_ref");
CREATE INDEX IF NOT EXISTS "idx_user_playbooks_governance_subject_ref"
    ON "public"."user_playbooks" ("governance_subject_ref");
CREATE INDEX IF NOT EXISTS "idx_eval_governance_subject_ref"
    ON "public"."agent_success_evaluation_result" ("governance_subject_ref");

CREATE TABLE IF NOT EXISTS "public"."session_outcomes" (
    "user_id" text NOT NULL,
    "session_id" text NOT NULL,
    "outcome" text NOT NULL CHECK ("outcome" IN ('success', 'failure')),
    "occurred_at" bigint NOT NULL,
    "source" text NOT NULL,
    "label" text,
    "value" double precision,
    "metadata" jsonb,
    "governance_subject_ref" text NOT NULL,
    "created_at" bigint NOT NULL,
    PRIMARY KEY ("user_id", "session_id")
);

CREATE INDEX IF NOT EXISTS "idx_session_outcomes_session_id"
    ON "public"."session_outcomes" ("session_id");
CREATE INDEX IF NOT EXISTS "idx_session_outcomes_subject_ref"
    ON "public"."session_outcomes" ("governance_subject_ref");

CREATE TABLE IF NOT EXISTS "public"."retrieved_learning_evaluation" (
    "result_id" bigserial PRIMARY KEY,
    "user_id" text NOT NULL,
    "session_id" text NOT NULL,
    "agent_version" text NOT NULL DEFAULT '',
    "interaction_id" bigint,
    "interaction_created_at" bigint,
    "kind" text NOT NULL,
    "learning_id" text NOT NULL,
    "is_relevant" boolean,
    "relevance_reason" text NOT NULL DEFAULT '',
    "impact" text,
    "impact_reason" text NOT NULL DEFAULT '',
    "created_at" bigint NOT NULL,
    "governance_subject_ref" text,
    UNIQUE ("user_id", "session_id", "interaction_id", "kind", "learning_id")
);

CREATE INDEX IF NOT EXISTS "idx_rle_created_at_result_id"
    ON "public"."retrieved_learning_evaluation" ("created_at" DESC, "result_id" DESC);
CREATE INDEX IF NOT EXISTS "idx_rle_interaction_created_at"
    ON "public"."retrieved_learning_evaluation" (
        "interaction_created_at" DESC, "interaction_id" DESC, "result_id" DESC
    );
CREATE INDEX IF NOT EXISTS "idx_rle_subject_ref"
    ON "public"."retrieved_learning_evaluation" ("governance_subject_ref");

CREATE TABLE IF NOT EXISTS "public"."learning_jobs" (
    "job_id" text PRIMARY KEY,
    "org_id" text NOT NULL,
    "user_id" text NOT NULL,
    "job_type" text NOT NULL DEFAULT 'learning',
    "latest_request_id" text,
    "status" text NOT NULL DEFAULT 'pending',
    "attempts" integer NOT NULL DEFAULT 0,
    "max_attempts" integer NOT NULL DEFAULT 3,
    "claimed_by" text,
    "claim_token" text,
    "claim_expires_at" timestamptz,
    "covers_through" timestamptz,
    "force_extraction" boolean NOT NULL DEFAULT false,
    "skip_aggregation" boolean NOT NULL DEFAULT false,
    "created_at" timestamptz NOT NULL DEFAULT now(),
    "updated_at" timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS "learning_jobs_coalesce"
    ON "public"."learning_jobs" ("org_id", "user_id", "job_type")
    WHERE "status" = 'pending';
CREATE INDEX IF NOT EXISTS "learning_jobs_poll"
    ON "public"."learning_jobs" ("created_at")
    WHERE "status" IN ('pending', 'failed', 'claimed');

CREATE TABLE IF NOT EXISTS "public"."subject_write_barriers" (
    "org_id" text NOT NULL,
    "subject_ref" text NOT NULL,
    "purge_id" text NOT NULL,
    "status" text NOT NULL CHECK ("status" IN ('erasing', 'erased', 'failed')),
    "error_code" text,
    "error_detail" text,
    "created_at" bigint NOT NULL,
    "updated_at" bigint NOT NULL,
    PRIMARY KEY ("org_id", "subject_ref"),
    FOREIGN KEY ("org_id", "purge_id")
        REFERENCES "public"."purge_operations" ("org_id", "purge_id")
        ON DELETE CASCADE
);
