ALTER TABLE "public"."agent_playbook_source_user_playbooks"
    ADD COLUMN IF NOT EXISTS "source_interaction_ids" bigint[] DEFAULT '{}'::bigint[] NOT NULL;

ALTER TABLE "public"."agent_playbook_source_user_playbooks"
    DROP CONSTRAINT IF EXISTS "agent_playbook_source_user_playbooks_user_playbook_id_fkey";

CREATE INDEX IF NOT EXISTS "idx_apsup_user"
    ON "public"."agent_playbook_source_user_playbooks" ("user_playbook_id");
