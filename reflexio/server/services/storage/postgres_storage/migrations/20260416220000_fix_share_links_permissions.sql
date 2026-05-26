-- Fix share_links permissions so the backend can INSERT/UPDATE/DELETE.
--
-- The original share_links migration (20260415120000) enabled RLS with a
-- policy restricted to auth.role() = 'service_role'. In practice this broke
-- every INSERT because the backend's Supabase client in our deployment is
-- not treated as service_role by PostgREST — every POST /api/share-links
-- returned 500 with "new row violates row-level security policy for
-- table share_links".
--
-- Every other table in this schema (interactions, profiles, agent_playbooks,
-- etc.) follows a different pattern: no RLS, and GRANT ALL to anon,
-- authenticated, and service_role. Access control is enforced in the
-- application layer via org_id scoping, not via Postgres RLS. This
-- migration brings share_links into line with that pattern.

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

-- Drop the restrictive policy and disable RLS.
DROP POLICY IF EXISTS "Service role has full access" ON "public"."share_links";
ALTER TABLE "public"."share_links" DISABLE ROW LEVEL SECURITY;

-- Expand grants to match the rest of the schema.
GRANT ALL ON TABLE "public"."share_links" TO "anon";
GRANT ALL ON TABLE "public"."share_links" TO "authenticated";
GRANT ALL ON TABLE "public"."share_links" TO "service_role";

GRANT ALL ON SEQUENCE "public"."share_links_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."share_links_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."share_links_id_seq" TO "service_role";
