
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

CREATE TABLE IF NOT EXISTS "public"."share_links" (
    "id" bigserial PRIMARY KEY,
    "org_id" character varying(50) NOT NULL,
    "token" character varying(100) NOT NULL UNIQUE,
    "resource_type" character varying(30) NOT NULL,
    "resource_id" character varying(255) NOT NULL,
    "created_at" integer DEFAULT (EXTRACT(epoch FROM "now"()))::integer,
    "expires_at" integer,
    "created_by_email" character varying(255)
);

ALTER TABLE "public"."share_links" OWNER TO "postgres";

CREATE INDEX IF NOT EXISTS "idx_share_links_org_id" ON "public"."share_links" USING "btree" ("org_id");
CREATE INDEX IF NOT EXISTS "idx_share_links_resource" ON "public"."share_links" USING "btree" ("resource_type", "resource_id");

-- Match the grants pattern of every other table in this schema
-- (interactions, profiles, etc.): GRANT ALL to all roles and do NOT
-- enable RLS. Access control is enforced in the application layer via
-- org_id scoping, not via Postgres RLS. The earlier service_role-only
-- RLS policy was corrected in migration 20260416220000; duplicating
-- that correction here keeps fresh installs consistent without relying
-- on the follow-up migration being applied.
GRANT ALL ON TABLE "public"."share_links" TO "anon";
GRANT ALL ON TABLE "public"."share_links" TO "authenticated";
GRANT ALL ON TABLE "public"."share_links" TO "service_role";

GRANT ALL ON SEQUENCE "public"."share_links_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."share_links_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."share_links_id_seq" TO "service_role";
