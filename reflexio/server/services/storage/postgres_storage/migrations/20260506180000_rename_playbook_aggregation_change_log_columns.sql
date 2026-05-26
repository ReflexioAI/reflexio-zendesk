-- Rename playbook_aggregation_change_logs columns from feedbacks -> playbooks.
--
-- The table was created with `*_feedbacks` column names from a prior naming
-- era, but the Python writer (playbook_aggregation_change_log_to_data) and
-- reader (response_to_playbook_aggregation_change_log) both use the
-- `*_playbooks` form. Without this rename, every aggregation run hits
-- "column does not exist" on insert.
--
-- The table also lives in every per-org schema provisioned via the
-- platform-managed flow, so the rename has to run in each of those schemas
-- in addition to public. The DO block below renames the columns wherever the
-- table currently exposes the old names.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'playbook_aggregation_change_logs'
          AND column_name = 'added_feedbacks'
    ) THEN
        ALTER TABLE IF EXISTS "public"."playbook_aggregation_change_logs"
            RENAME COLUMN "added_feedbacks" TO "added_playbooks";
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'playbook_aggregation_change_logs'
          AND column_name = 'removed_feedbacks'
    ) THEN
        ALTER TABLE IF EXISTS "public"."playbook_aggregation_change_logs"
            RENAME COLUMN "removed_feedbacks" TO "removed_playbooks";
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'playbook_aggregation_change_logs'
          AND column_name = 'updated_feedbacks'
    ) THEN
        ALTER TABLE IF EXISTS "public"."playbook_aggregation_change_logs"
            RENAME COLUMN "updated_feedbacks" TO "updated_playbooks";
    END IF;
END
$$;

NOTIFY pgrst, 'reload schema';
