-- Mirror upstream SQLite storage contract additions for native Postgres.

ALTER TABLE "public"."profiles"
    ADD COLUMN IF NOT EXISTS "tags" jsonb,
    ADD COLUMN IF NOT EXISTS "source_interaction_ids" jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS "merged_into" text,
    ADD COLUMN IF NOT EXISTS "superseded_by" text,
    ADD COLUMN IF NOT EXISTS "retired_at" bigint;

ALTER TABLE "public"."user_playbooks"
    ADD COLUMN IF NOT EXISTS "tags" jsonb,
    ADD COLUMN IF NOT EXISTS "merged_into" bigint,
    ADD COLUMN IF NOT EXISTS "superseded_by" bigint,
    ADD COLUMN IF NOT EXISTS "retired_at" bigint;

ALTER TABLE "public"."agent_playbooks"
    ADD COLUMN IF NOT EXISTS "tags" jsonb,
    ADD COLUMN IF NOT EXISTS "merged_into" bigint,
    ADD COLUMN IF NOT EXISTS "superseded_by" bigint,
    ADD COLUMN IF NOT EXISTS "retired_at" bigint;

CREATE INDEX IF NOT EXISTS "idx_profiles_retired_at"
    ON "public"."profiles" ("status", "retired_at");
CREATE INDEX IF NOT EXISTS "idx_user_playbooks_retired_at"
    ON "public"."user_playbooks" ("status", "retired_at");
CREATE INDEX IF NOT EXISTS "idx_agent_playbooks_retired_at"
    ON "public"."agent_playbooks" ("status", "retired_at");

CREATE TABLE IF NOT EXISTS "public"."lineage_event" (
    "event_id" bigserial PRIMARY KEY,
    "org_id" text NOT NULL,
    "entity_type" text NOT NULL,
    "entity_id" text NOT NULL,
    "op" text NOT NULL,
    "prov_relation" text NOT NULL DEFAULT '',
    "source_ids" jsonb NOT NULL DEFAULT '[]'::jsonb,
    "actor" text NOT NULL DEFAULT '',
    "request_id" text NOT NULL DEFAULT '',
    "reason" text NOT NULL DEFAULT '',
    "created_at" bigint NOT NULL,
    "from_status" text,
    "to_status" text,
    "status_namespace" text,
    UNIQUE ("org_id", "entity_type", "entity_id", "op", "request_id")
);

CREATE INDEX IF NOT EXISTS "idx_lineage_entity"
    ON "public"."lineage_event" ("entity_type", "entity_id");
CREATE INDEX IF NOT EXISTS "idx_lineage_org_request"
    ON "public"."lineage_event" ("org_id", "request_id", "event_id");

CREATE TABLE IF NOT EXISTS "public"."playbook_retrieval_logs" (
    "retrieval_log_id" bigserial PRIMARY KEY,
    "org_id" text NOT NULL,
    "request_id" text NOT NULL,
    "session_id" text NOT NULL,
    "interaction_id" bigint,
    "user_id" text NOT NULL,
    "query" text,
    "agent_version" text,
    "created_at" bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS "public"."playbook_retrieval_log_items" (
    "retrieval_log_item_id" bigserial PRIMARY KEY,
    "retrieval_log_id" bigint NOT NULL REFERENCES "public"."playbook_retrieval_logs"("retrieval_log_id") ON DELETE CASCADE,
    "ordinal" integer NOT NULL,
    "agent_playbook_id" bigint NOT NULL,
    "source_user_playbook_ids" jsonb NOT NULL DEFAULT '[]'::jsonb,
    "source_interaction_ids_by_user_playbook_id" jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS "idx_playbook_retrieval_logs_org_user_created"
    ON "public"."playbook_retrieval_logs" ("org_id", "user_id", "created_at", "retrieval_log_id");
CREATE INDEX IF NOT EXISTS "idx_playbook_retrieval_logs_org_request"
    ON "public"."playbook_retrieval_logs" ("org_id", "request_id");
CREATE INDEX IF NOT EXISTS "idx_playbook_retrieval_log_items_log"
    ON "public"."playbook_retrieval_log_items" ("retrieval_log_id", "ordinal");

CREATE TABLE IF NOT EXISTS "public"."audit_events" (
    "event_id" bigserial PRIMARY KEY,
    "org_id" text NOT NULL,
    "actor_type" text NOT NULL DEFAULT 'system',
    "actor_ref" text,
    "operation" text NOT NULL,
    "entity_type" text NOT NULL,
    "entity_id" text,
    "subject_ref" text,
    "request_ref" text NOT NULL,
    "idempotency_key" text,
    "status" text NOT NULL DEFAULT 'ok',
    "detail" jsonb,
    "created_at" bigint NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS "idx_audit_events_org_idem"
    ON "public"."audit_events" ("org_id", "idempotency_key")
    WHERE "idempotency_key" IS NOT NULL;
CREATE INDEX IF NOT EXISTS "idx_audit_events_subject_created"
    ON "public"."audit_events" ("org_id", "subject_ref", "created_at", "event_id");
CREATE INDEX IF NOT EXISTS "idx_audit_events_org_created"
    ON "public"."audit_events" ("org_id", "created_at", "event_id");

CREATE TABLE IF NOT EXISTS "public"."purge_operations" (
    "org_id" text NOT NULL,
    "purge_id" text NOT NULL,
    "operation_type" text NOT NULL,
    "scope_type" text NOT NULL,
    "subject_ref" text,
    "request_ref" text NOT NULL,
    "idempotency_key" text NOT NULL,
    "status" text NOT NULL DEFAULT 'pending',
    "error_code" text,
    "error_detail" text,
    "created_at" bigint NOT NULL,
    "updated_at" bigint NOT NULL,
    "completed_at" bigint,
    PRIMARY KEY ("org_id", "purge_id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "idx_purge_operations_org_idem"
    ON "public"."purge_operations" ("org_id", "idempotency_key");

CREATE TABLE IF NOT EXISTS "public"."purge_operation_targets" (
    "org_id" text NOT NULL,
    "purge_id" text NOT NULL,
    "target_name" text NOT NULL,
    "target_ref" text NOT NULL DEFAULT '',
    "phase" text NOT NULL,
    "status" text NOT NULL DEFAULT 'pending',
    "detail" jsonb,
    "deleted_count" bigint NOT NULL DEFAULT 0,
    "error_detail" text,
    "started_at" bigint,
    "completed_at" bigint,
    PRIMARY KEY ("org_id", "purge_id", "target_name", "target_ref", "phase"),
    FOREIGN KEY ("org_id", "purge_id")
        REFERENCES "public"."purge_operations"("org_id", "purge_id")
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS "idx_purge_targets_purge_phase"
    ON "public"."purge_operation_targets" ("org_id", "purge_id", "phase", "status");
