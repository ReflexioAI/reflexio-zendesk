-- Fix _operation_state permissions for managed Supabase deployments.
--
-- _operation_state stores extraction progress, locks, and pending request
-- queues. It follows the same app-layer access-control model as the rest of
-- the data schema: no table-level RLS, with access granted to the roles used
-- by PostgREST. If RLS is enabled in a managed project, publish extraction can
-- write the interaction but fail before profile/playbook generation with:
--
--   new row violates row-level security policy for table "_operation_state"
--
-- Make the intended permission model explicit so drifted or dashboard-created
-- managed projects recover when this migration is applied.

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

ALTER TABLE IF EXISTS "public"."_operation_state" DISABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS "public"."_operation_state" NO FORCE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."_operation_state" TO "anon";
GRANT ALL ON TABLE "public"."_operation_state" TO "authenticated";
GRANT ALL ON TABLE "public"."_operation_state" TO "service_role";

GRANT ALL ON FUNCTION "public"."try_acquire_in_progress_lock"(
    "p_state_key" "text",
    "p_request_id" "text",
    "p_stale_lock_seconds" integer,
    "p_payload" "jsonb"
) TO "anon";
GRANT ALL ON FUNCTION "public"."try_acquire_in_progress_lock"(
    "p_state_key" "text",
    "p_request_id" "text",
    "p_stale_lock_seconds" integer,
    "p_payload" "jsonb"
) TO "authenticated";
GRANT ALL ON FUNCTION "public"."try_acquire_in_progress_lock"(
    "p_state_key" "text",
    "p_request_id" "text",
    "p_stale_lock_seconds" integer,
    "p_payload" "jsonb"
) TO "service_role";
