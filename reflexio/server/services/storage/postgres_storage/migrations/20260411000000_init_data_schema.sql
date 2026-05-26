
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

CREATE EXTENSION IF NOT EXISTS "pg_net" WITH SCHEMA "extensions";

COMMENT ON SCHEMA "public" IS 'standard public schema';

CREATE EXTENSION IF NOT EXISTS "pg_graphql" WITH SCHEMA "graphql";

CREATE EXTENSION IF NOT EXISTS "pg_stat_statements" WITH SCHEMA "extensions";

CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA "extensions";

CREATE EXTENSION IF NOT EXISTS "supabase_vault" WITH SCHEMA "vault";

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA "extensions";

CREATE EXTENSION IF NOT EXISTS "vector" WITH SCHEMA "public";

CREATE OR REPLACE FUNCTION "public"."get_last_k_interactions"("p_user_id" "text" DEFAULT NULL::"text", "p_limit" integer DEFAULT 100, "p_sources" "text"[] DEFAULT NULL::"text"[], "p_start_time" bigint DEFAULT NULL::bigint, "p_end_time" bigint DEFAULT NULL::bigint, "p_agent_version" "text" DEFAULT NULL::"text") RETURNS TABLE("request_id" "text", "request_user_id" "text", "request_created_at" timestamp with time zone, "request_source" "text", "request_agent_version" "text", "session_id" "text", "interaction_id" bigint, "interaction_user_id" "text", "interaction_content" "text", "interaction_request_id" "text", "interaction_created_at" timestamp with time zone, "interaction_role" "text", "interaction_user_action" "text", "interaction_user_action_description" "text", "interaction_interacted_image_url" "text", "interaction_shadow_content" "text", "interaction_expert_content" "text", "interaction_tools_used" "jsonb")
    LANGUAGE "plpgsql"
    AS $$
BEGIN
    RETURN QUERY
    WITH last_k_interactions AS (
        SELECT i.*
        FROM interactions i
        INNER JOIN requests r ON i.request_id = r.request_id
        WHERE (p_user_id IS NULL OR i.user_id = p_user_id)
          AND (p_sources IS NULL OR r.source = ANY(p_sources))
          AND (p_start_time IS NULL OR i.created_at >= to_timestamp(p_start_time))
          AND (p_end_time IS NULL OR i.created_at <= to_timestamp(p_end_time))
          AND (p_agent_version IS NULL OR r.agent_version = p_agent_version)
        ORDER BY i.interaction_id DESC
        LIMIT p_limit
    )
    SELECT
        r.request_id,
        r.user_id as request_user_id,
        r.created_at as request_created_at,
        r.source as request_source,
        r.agent_version as request_agent_version,
        r.session_id,
        lki.interaction_id,
        lki.user_id as interaction_user_id,
        lki.content as interaction_content,
        lki.request_id as interaction_request_id,
        lki.created_at as interaction_created_at,
        lki.role as interaction_role,
        lki.user_action as interaction_user_action,
        lki.user_action_description as interaction_user_action_description,
        lki.interacted_image_url as interaction_interacted_image_url,
        lki.shadow_content as interaction_shadow_content,
        lki.expert_content as interaction_expert_content,
        lki.tools_used as interaction_tools_used
    FROM last_k_interactions lki
    INNER JOIN requests r ON lki.request_id = r.request_id
    ORDER BY lki.interaction_id DESC;
END;
$$;

ALTER FUNCTION "public"."get_last_k_interactions"("p_user_id" "text", "p_limit" integer, "p_sources" "text"[], "p_start_time" bigint, "p_end_time" bigint, "p_agent_version" "text") OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."get_new_request_interaction_groups"("p_user_id" "text" DEFAULT NULL::"text", "p_last_processed_timestamp" timestamp with time zone DEFAULT NULL::timestamp with time zone, "p_excluded_interaction_ids" bigint[] DEFAULT ARRAY[]::bigint[], "p_sources" "text"[] DEFAULT NULL::"text"[]) RETURNS TABLE("request_id" "text", "request_user_id" "text", "request_created_at" timestamp with time zone, "request_source" "text", "request_agent_version" "text", "session_id" "text", "interaction_id" bigint, "interaction_user_id" "text", "interaction_content" "text", "interaction_request_id" "text", "interaction_created_at" timestamp with time zone, "interaction_role" "text", "interaction_user_action" "text", "interaction_user_action_description" "text", "interaction_interacted_image_url" "text", "interaction_shadow_content" "text", "interaction_expert_content" "text", "interaction_tools_used" "jsonb")
    LANGUAGE "plpgsql"
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        r.request_id,
        r.user_id as request_user_id,
        r.created_at as request_created_at,
        r.source as request_source,
        r.agent_version as request_agent_version,
        r.session_id,
        i.interaction_id,
        i.user_id as interaction_user_id,
        i.content as interaction_content,
        i.request_id as interaction_request_id,
        i.created_at as interaction_created_at,
        i.role as interaction_role,
        i.user_action as interaction_user_action,
        i.user_action_description as interaction_user_action_description,
        i.interacted_image_url as interaction_interacted_image_url,
        i.shadow_content as interaction_shadow_content,
        i.expert_content as interaction_expert_content,
        i.tools_used as interaction_tools_used
    FROM requests r
    INNER JOIN interactions i ON r.request_id = i.request_id
    WHERE (p_user_id IS NULL OR r.user_id = p_user_id)
      AND (p_user_id IS NULL OR i.user_id = p_user_id)
      AND (p_last_processed_timestamp IS NULL OR i.created_at >= p_last_processed_timestamp)
      AND NOT (i.interaction_id = ANY(p_excluded_interaction_ids))
      AND (p_sources IS NULL OR r.source = ANY(p_sources))
    ORDER BY i.interaction_id ASC;
END;
$$;

ALTER FUNCTION "public"."get_new_request_interaction_groups"("p_user_id" "text", "p_last_processed_timestamp" timestamp with time zone, "p_excluded_interaction_ids" bigint[], "p_sources" "text"[]) OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."hybrid_match_agent_playbooks"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision DEFAULT 0.7, "p_match_count" integer DEFAULT 10, "p_search_mode" "text" DEFAULT 'hybrid'::"text", "p_rrf_k" integer DEFAULT 60, "p_vector_weight" double precision DEFAULT 1.0, "p_fts_weight" double precision DEFAULT 1.0) RETURNS TABLE("agent_playbook_id" bigint, "playbook_name" "text", "content" "text", "structured_data" "jsonb", "playbook_status" "text", "agent_version" "text", "playbook_metadata" "text", "created_at" timestamp with time zone, "status" "text", "similarity" double precision, "fts_rank" double precision, "combined_score" double precision)
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    tsquery_val tsquery;
BEGIN
    tsquery_val := websearch_to_tsquery('english', p_query_text);

    RETURN QUERY
    WITH
    vector_results AS (
        SELECT
            ap.agent_playbook_id,
            ap.playbook_name,
            ap.content,
            ap.structured_data,
            ap.playbook_status,
            ap.agent_version,
            ap.playbook_metadata,
            ap.created_at,
            ap.status,
            1 - (ap.embedding <=> p_query_embedding) as vec_similarity,
            ROW_NUMBER() OVER (ORDER BY ap.embedding <=> p_query_embedding) as vec_rank
        FROM agent_playbooks ap
        WHERE (p_search_mode = 'fts' OR 1 - (ap.embedding <=> p_query_embedding) > p_match_threshold)
          AND ap.status IS NULL
        ORDER BY ap.embedding <=> p_query_embedding
        LIMIT CASE WHEN p_search_mode = 'fts' THEN 0 ELSE p_match_count * 3 END
    ),
    fts_results AS (
        SELECT
            ap.agent_playbook_id,
            ap.playbook_name,
            ap.content,
            ap.structured_data,
            ap.playbook_status,
            ap.agent_version,
            ap.playbook_metadata,
            ap.created_at,
            ap.status,
            ts_rank_cd(ap.search_fts, tsquery_val, 1)::double precision as fts_score,
            ROW_NUMBER() OVER (ORDER BY ts_rank_cd(ap.search_fts, tsquery_val, 1) DESC) as fts_rank
        FROM agent_playbooks ap
        WHERE (p_search_mode = 'vector' OR ap.search_fts @@ tsquery_val)
          AND ap.status IS NULL
        ORDER BY ts_rank_cd(ap.search_fts, tsquery_val, 1) DESC
        LIMIT CASE WHEN p_search_mode = 'vector' THEN 0 ELSE p_match_count * 3 END
    ),
    combined AS (
        SELECT
            COALESCE(v.agent_playbook_id, f.agent_playbook_id) as agent_playbook_id,
            COALESCE(v.playbook_name, f.playbook_name) as playbook_name,
            COALESCE(v.content, f.content) as content,
            COALESCE(v.structured_data, f.structured_data) as structured_data,
            COALESCE(v.playbook_status, f.playbook_status) as playbook_status,
            COALESCE(v.agent_version, f.agent_version) as agent_version,
            COALESCE(v.playbook_metadata, f.playbook_metadata) as playbook_metadata,
            COALESCE(v.created_at, f.created_at) as created_at,
            COALESCE(v.status, f.status) as status,
            v.vec_similarity as similarity,
            f.fts_score as fts_rank,
            CASE
                WHEN p_search_mode = 'vector' THEN COALESCE(v.vec_similarity, 0)
                WHEN p_search_mode = 'fts' THEN COALESCE(f.fts_score, 0)
                ELSE
                    p_vector_weight * COALESCE(1.0 / (p_rrf_k + v.vec_rank), 0) +
                    p_fts_weight * COALESCE(1.0 / (p_rrf_k + f.fts_rank), 0)
            END as combined_score
        FROM vector_results v
        FULL OUTER JOIN fts_results f ON v.agent_playbook_id = f.agent_playbook_id
    )
    SELECT
        c.agent_playbook_id,
        c.playbook_name,
        c.content,
        c.structured_data,
        c.playbook_status,
        c.agent_version,
        c.playbook_metadata,
        c.created_at,
        c.status,
        c.similarity,
        c.fts_rank,
        c.combined_score
    FROM combined c
    ORDER BY c.combined_score DESC
    LIMIT p_match_count;
END;
$$;

ALTER FUNCTION "public"."hybrid_match_agent_playbooks"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."hybrid_match_interactions"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision DEFAULT 0.1, "p_match_count" integer DEFAULT 10, "p_search_mode" "text" DEFAULT 'hybrid'::"text", "p_rrf_k" integer DEFAULT 60, "p_vector_weight" double precision DEFAULT 1.0, "p_fts_weight" double precision DEFAULT 1.0) RETURNS TABLE("interaction_id" bigint, "user_id" "text", "content" "text", "request_id" "text", "created_at" timestamp with time zone, "user_action" "text", "user_action_description" "text", "interacted_image_url" "text", "similarity" double precision, "fts_rank" double precision, "combined_score" double precision)
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    tsquery_val tsquery;
BEGIN
    tsquery_val := websearch_to_tsquery('english', p_query_text);

    RETURN QUERY
    WITH
    vector_results AS (
        SELECT
            i.interaction_id,
            i.user_id,
            i.content,
            i.request_id,
            i.created_at,
            i.user_action,
            i.user_action_description,
            i.interacted_image_url,
            1 - (i.embedding <=> p_query_embedding) as vec_similarity,
            ROW_NUMBER() OVER (ORDER BY i.embedding <=> p_query_embedding) as vec_rank
        FROM interactions i
        WHERE (p_search_mode = 'fts' OR 1 - (i.embedding <=> p_query_embedding) > p_match_threshold)
        ORDER BY i.embedding <=> p_query_embedding
        LIMIT CASE WHEN p_search_mode = 'fts' THEN 0 ELSE p_match_count * 3 END
    ),
    fts_results AS (
        SELECT
            i.interaction_id,
            i.user_id,
            i.content,
            i.request_id,
            i.created_at,
            i.user_action,
            i.user_action_description,
            i.interacted_image_url,
            ts_rank_cd(i.content_fts, tsquery_val, 1)::double precision as fts_score,
            ROW_NUMBER() OVER (ORDER BY ts_rank_cd(i.content_fts, tsquery_val, 1) DESC) as fts_rank
        FROM interactions i
        WHERE (p_search_mode = 'vector' OR i.content_fts @@ tsquery_val)
        ORDER BY ts_rank_cd(i.content_fts, tsquery_val, 1) DESC
        LIMIT CASE WHEN p_search_mode = 'vector' THEN 0 ELSE p_match_count * 3 END
    ),
    combined AS (
        SELECT
            COALESCE(v.interaction_id, f.interaction_id) as interaction_id,
            COALESCE(v.user_id, f.user_id) as user_id,
            COALESCE(v.content, f.content) as content,
            COALESCE(v.request_id, f.request_id) as request_id,
            COALESCE(v.created_at, f.created_at) as created_at,
            COALESCE(v.user_action, f.user_action) as user_action,
            COALESCE(v.user_action_description, f.user_action_description) as user_action_description,
            COALESCE(v.interacted_image_url, f.interacted_image_url) as interacted_image_url,
            v.vec_similarity as similarity,
            f.fts_score as fts_rank,
            CASE
                WHEN p_search_mode = 'vector' THEN COALESCE(v.vec_similarity, 0)
                WHEN p_search_mode = 'fts' THEN COALESCE(f.fts_score, 0)
                ELSE
                    p_vector_weight * COALESCE(1.0 / (p_rrf_k + v.vec_rank), 0) +
                    p_fts_weight * COALESCE(1.0 / (p_rrf_k + f.fts_rank), 0)
            END as combined_score
        FROM vector_results v
        FULL OUTER JOIN fts_results f ON v.interaction_id = f.interaction_id
    )
    SELECT
        c.interaction_id,
        c.user_id,
        c.content,
        c.request_id,
        c.created_at,
        c.user_action,
        c.user_action_description,
        c.interacted_image_url,
        c.similarity,
        c.fts_rank,
        c.combined_score
    FROM combined c
    ORDER BY c.combined_score DESC
    LIMIT p_match_count;
END;
$$;

ALTER FUNCTION "public"."hybrid_match_interactions"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."hybrid_match_profiles"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision DEFAULT 0.3, "p_match_count" integer DEFAULT 10, "p_current_epoch" bigint DEFAULT 0, "p_filter_user_id" "text" DEFAULT NULL::"text", "p_search_mode" "text" DEFAULT 'hybrid'::"text", "p_rrf_k" integer DEFAULT 60, "p_filter_extractor_name" "text" DEFAULT NULL::"text", "p_vector_weight" double precision DEFAULT 1.0, "p_fts_weight" double precision DEFAULT 1.0) RETURNS TABLE("profile_id" "text", "user_id" "text", "content" "text", "last_modified_timestamp" bigint, "generated_from_request_id" "text", "profile_time_to_live" "text", "expiration_timestamp" bigint, "custom_features" json, "source" character varying, "status" "text", "extractor_names" json, "similarity" double precision, "fts_rank" double precision, "combined_score" double precision)
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    tsquery_val tsquery;
BEGIN
    tsquery_val := websearch_to_tsquery('english', p_query_text);

    RETURN QUERY
    WITH
    vector_results AS (
        SELECT
            p.profile_id,
            p.user_id,
            p.content,
            p.last_modified_timestamp,
            p.generated_from_request_id,
            p.profile_time_to_live,
            p.expiration_timestamp,
            p.custom_features,
            p.source,
            p.status,
            p.extractor_names,
            1 - (p.embedding <=> p_query_embedding) as vec_similarity,
            ROW_NUMBER() OVER (ORDER BY p.embedding <=> p_query_embedding) as vec_rank
        FROM profiles p
        WHERE p.expiration_timestamp >= p_current_epoch
          AND (p_search_mode = 'fts' OR 1 - (p.embedding <=> p_query_embedding) > p_match_threshold)
          AND (p_filter_user_id IS NULL OR p.user_id = p_filter_user_id)
          AND (p_filter_extractor_name IS NULL OR p.extractor_names::jsonb @> to_jsonb(p_filter_extractor_name))
          AND p.status IS NULL
        ORDER BY p.embedding <=> p_query_embedding
        LIMIT CASE WHEN p_search_mode = 'fts' THEN 0 ELSE p_match_count * 3 END
    ),
    fts_results AS (
        SELECT
            p.profile_id,
            p.user_id,
            p.content,
            p.last_modified_timestamp,
            p.generated_from_request_id,
            p.profile_time_to_live,
            p.expiration_timestamp,
            p.custom_features,
            p.source,
            p.status,
            p.extractor_names,
            ts_rank_cd(p.content_fts, tsquery_val, 1)::double precision as fts_score,
            ROW_NUMBER() OVER (ORDER BY ts_rank_cd(p.content_fts, tsquery_val, 1) DESC) as fts_rank
        FROM profiles p
        WHERE p.expiration_timestamp >= p_current_epoch
          AND (p_search_mode = 'vector' OR p.content_fts @@ tsquery_val)
          AND (p_filter_user_id IS NULL OR p.user_id = p_filter_user_id)
          AND (p_filter_extractor_name IS NULL OR p.extractor_names::jsonb @> to_jsonb(p_filter_extractor_name))
          AND p.status IS NULL
        ORDER BY ts_rank_cd(p.content_fts, tsquery_val, 1) DESC
        LIMIT CASE WHEN p_search_mode = 'vector' THEN 0 ELSE p_match_count * 3 END
    ),
    combined AS (
        SELECT
            COALESCE(v.profile_id, f.profile_id) as profile_id,
            COALESCE(v.user_id, f.user_id) as user_id,
            COALESCE(v.content, f.content) as content,
            COALESCE(v.last_modified_timestamp, f.last_modified_timestamp) as last_modified_timestamp,
            COALESCE(v.generated_from_request_id, f.generated_from_request_id) as generated_from_request_id,
            COALESCE(v.profile_time_to_live, f.profile_time_to_live) as profile_time_to_live,
            COALESCE(v.expiration_timestamp, f.expiration_timestamp) as expiration_timestamp,
            COALESCE(v.custom_features, f.custom_features) as custom_features,
            COALESCE(v.source, f.source) as source,
            COALESCE(v.status, f.status) as status,
            COALESCE(v.extractor_names, f.extractor_names) as extractor_names,
            v.vec_similarity as similarity,
            f.fts_score as fts_rank,
            CASE
                WHEN p_search_mode = 'vector' THEN COALESCE(v.vec_similarity, 0)
                WHEN p_search_mode = 'fts' THEN COALESCE(f.fts_score, 0)
                ELSE
                    p_vector_weight * COALESCE(1.0 / (p_rrf_k + v.vec_rank), 0) +
                    p_fts_weight * COALESCE(1.0 / (p_rrf_k + f.fts_rank), 0)
            END as combined_score
        FROM vector_results v
        FULL OUTER JOIN fts_results f ON v.profile_id = f.profile_id
    )
    SELECT
        c.profile_id,
        c.user_id,
        c.content,
        c.last_modified_timestamp,
        c.generated_from_request_id,
        c.profile_time_to_live,
        c.expiration_timestamp,
        c.custom_features,
        c.source,
        c.status,
        c.extractor_names,
        c.similarity,
        c.fts_rank,
        c.combined_score
    FROM combined c
    ORDER BY c.combined_score DESC
    LIMIT p_match_count;
END;
$$;

ALTER FUNCTION "public"."hybrid_match_profiles"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_current_epoch" bigint, "p_filter_user_id" "text", "p_search_mode" "text", "p_rrf_k" integer, "p_filter_extractor_name" "text", "p_vector_weight" double precision, "p_fts_weight" double precision) OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."hybrid_match_skills"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision DEFAULT 0.3, "p_match_count" integer DEFAULT 10, "p_org_id" "text" DEFAULT NULL::"text", "p_search_mode" "text" DEFAULT 'hybrid'::"text", "p_rrf_k" integer DEFAULT 60, "p_vector_weight" double precision DEFAULT 1.0, "p_fts_weight" double precision DEFAULT 1.0) RETURNS TABLE("skill_id" bigint, "org_id" "text", "skill_name" "text", "description" "text", "version" "text", "agent_version" "text", "playbook_name" "text", "instructions" "text", "allowed_tools" "jsonb", "blocking_issues" "jsonb", "user_playbook_ids" "jsonb", "skill_status" "text", "embedding" "public"."vector", "created_at" timestamp with time zone, "updated_at" timestamp with time zone, "similarity" double precision, "fts_rank" double precision, "combined_score" double precision)
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    tsquery_val tsquery;
BEGIN
    tsquery_val := websearch_to_tsquery('english', p_query_text);

    RETURN QUERY
    WITH
    vector_results AS (
        SELECT
            s.skill_id,
            s.org_id,
            s.skill_name,
            s.description,
            s.version,
            s.agent_version,
            s.playbook_name,
            s.instructions,
            s.allowed_tools,
            s.blocking_issues,
            s.user_playbook_ids,
            s.skill_status,
            s.embedding,
            s.created_at,
            s.updated_at,
            1 - (s.embedding <=> p_query_embedding) as vec_similarity,
            ROW_NUMBER() OVER (ORDER BY s.embedding <=> p_query_embedding) as vec_rank
        FROM public.skills s
        WHERE (p_search_mode = 'fts' OR 1 - (s.embedding <=> p_query_embedding) > p_match_threshold)
          AND (p_org_id IS NULL OR s.org_id = p_org_id)
        ORDER BY s.embedding <=> p_query_embedding
        LIMIT CASE WHEN p_search_mode = 'fts' THEN 0 ELSE p_match_count * 3 END
    ),
    fts_results AS (
        SELECT
            s.skill_id,
            s.org_id,
            s.skill_name,
            s.description,
            s.version,
            s.agent_version,
            s.playbook_name,
            s.instructions,
            s.allowed_tools,
            s.blocking_issues,
            s.user_playbook_ids,
            s.skill_status,
            s.embedding,
            s.created_at,
            s.updated_at,
            ts_rank_cd(s.content_fts, tsquery_val, 1)::double precision as fts_score,
            ROW_NUMBER() OVER (ORDER BY ts_rank_cd(s.content_fts, tsquery_val, 1) DESC) as fts_rank
        FROM public.skills s
        WHERE (p_search_mode = 'vector' OR s.content_fts @@ tsquery_val)
          AND (p_org_id IS NULL OR s.org_id = p_org_id)
        ORDER BY ts_rank_cd(s.content_fts, tsquery_val, 1) DESC
        LIMIT CASE WHEN p_search_mode = 'vector' THEN 0 ELSE p_match_count * 3 END
    ),
    combined AS (
        SELECT
            COALESCE(v.skill_id, f.skill_id) as skill_id,
            COALESCE(v.org_id, f.org_id) as org_id,
            COALESCE(v.skill_name, f.skill_name) as skill_name,
            COALESCE(v.description, f.description) as description,
            COALESCE(v.version, f.version) as version,
            COALESCE(v.agent_version, f.agent_version) as agent_version,
            COALESCE(v.playbook_name, f.playbook_name) as playbook_name,
            COALESCE(v.instructions, f.instructions) as instructions,
            COALESCE(v.allowed_tools, f.allowed_tools) as allowed_tools,
            COALESCE(v.blocking_issues, f.blocking_issues) as blocking_issues,
            COALESCE(v.user_playbook_ids, f.user_playbook_ids) as user_playbook_ids,
            COALESCE(v.skill_status, f.skill_status) as skill_status,
            COALESCE(v.embedding, f.embedding) as embedding,
            COALESCE(v.created_at, f.created_at) as created_at,
            COALESCE(v.updated_at, f.updated_at) as updated_at,
            v.vec_similarity as similarity,
            f.fts_score as fts_rank,
            CASE
                WHEN p_search_mode = 'vector' THEN COALESCE(v.vec_similarity, 0)
                WHEN p_search_mode = 'fts' THEN COALESCE(f.fts_score, 0)
                ELSE
                    p_vector_weight * COALESCE(1.0 / (p_rrf_k + v.vec_rank), 0) +
                    p_fts_weight * COALESCE(1.0 / (p_rrf_k + f.fts_rank), 0)
            END as combined_score
        FROM vector_results v
        FULL OUTER JOIN fts_results f ON v.skill_id = f.skill_id
    )
    SELECT
        c.skill_id,
        c.org_id,
        c.skill_name,
        c.description,
        c.version,
        c.agent_version,
        c.playbook_name,
        c.instructions,
        c.allowed_tools,
        c.blocking_issues,
        c.user_playbook_ids,
        c.skill_status,
        c.embedding,
        c.created_at,
        c.updated_at,
        c.similarity,
        c.fts_rank,
        c.combined_score
    FROM combined c
    ORDER BY c.combined_score DESC
    LIMIT p_match_count;
END;
$$;

ALTER FUNCTION "public"."hybrid_match_skills"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_org_id" "text", "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."hybrid_match_user_playbooks"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision DEFAULT 0.7, "p_match_count" integer DEFAULT 10, "p_filter_user_id" "text" DEFAULT NULL::"text", "p_search_mode" "text" DEFAULT 'hybrid'::"text", "p_rrf_k" integer DEFAULT 60, "p_vector_weight" double precision DEFAULT 1.0, "p_fts_weight" double precision DEFAULT 1.0) RETURNS TABLE("user_playbook_id" bigint, "user_id" "text", "playbook_name" "text", "request_id" "text", "agent_version" "text", "content" "text", "structured_data" "jsonb", "source" "text", "status" "text", "source_interaction_ids" bigint[], "created_at" timestamp with time zone, "similarity" double precision, "fts_rank" double precision, "combined_score" double precision)
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    tsquery_val tsquery;
BEGIN
    tsquery_val := websearch_to_tsquery('english', p_query_text);

    RETURN QUERY
    WITH
    vector_results AS (
        SELECT
            up.user_playbook_id,
            up.user_id,
            up.playbook_name,
            up.request_id,
            up.agent_version,
            up.content,
            up.structured_data,
            up.source,
            up.status,
            up.source_interaction_ids,
            up.created_at,
            1 - (up.embedding <=> p_query_embedding) as vec_similarity,
            ROW_NUMBER() OVER (ORDER BY up.embedding <=> p_query_embedding) as vec_rank
        FROM user_playbooks up
        WHERE (p_search_mode = 'fts' OR 1 - (up.embedding <=> p_query_embedding) > p_match_threshold)
          AND up.status IS NULL
          AND (p_filter_user_id IS NULL OR up.user_id = p_filter_user_id)
        ORDER BY up.embedding <=> p_query_embedding
        LIMIT CASE WHEN p_search_mode = 'fts' THEN 0 ELSE p_match_count * 3 END
    ),
    fts_results AS (
        SELECT
            up.user_playbook_id,
            up.user_id,
            up.playbook_name,
            up.request_id,
            up.agent_version,
            up.content,
            up.structured_data,
            up.source,
            up.status,
            up.source_interaction_ids,
            up.created_at,
            ts_rank_cd(up.search_fts, tsquery_val, 1)::double precision as fts_score,
            ROW_NUMBER() OVER (ORDER BY ts_rank_cd(up.search_fts, tsquery_val, 1) DESC) as fts_rank
        FROM user_playbooks up
        WHERE (p_search_mode = 'vector' OR up.search_fts @@ tsquery_val)
          AND up.status IS NULL
          AND (p_filter_user_id IS NULL OR up.user_id = p_filter_user_id)
        ORDER BY ts_rank_cd(up.search_fts, tsquery_val, 1) DESC
        LIMIT CASE WHEN p_search_mode = 'vector' THEN 0 ELSE p_match_count * 3 END
    ),
    combined AS (
        SELECT
            COALESCE(v.user_playbook_id, f.user_playbook_id) as user_playbook_id,
            COALESCE(v.user_id, f.user_id) as user_id,
            COALESCE(v.playbook_name, f.playbook_name) as playbook_name,
            COALESCE(v.request_id, f.request_id) as request_id,
            COALESCE(v.agent_version, f.agent_version) as agent_version,
            COALESCE(v.content, f.content) as content,
            COALESCE(v.structured_data, f.structured_data) as structured_data,
            COALESCE(v.source, f.source) as source,
            COALESCE(v.status, f.status) as status,
            COALESCE(v.source_interaction_ids, f.source_interaction_ids) as source_interaction_ids,
            COALESCE(v.created_at, f.created_at) as created_at,
            v.vec_similarity as similarity,
            f.fts_score as fts_rank,
            CASE
                WHEN p_search_mode = 'vector' THEN COALESCE(v.vec_similarity, 0)
                WHEN p_search_mode = 'fts' THEN COALESCE(f.fts_score, 0)
                ELSE
                    p_vector_weight * COALESCE(1.0 / (p_rrf_k + v.vec_rank), 0) +
                    p_fts_weight * COALESCE(1.0 / (p_rrf_k + f.fts_rank), 0)
            END as combined_score
        FROM vector_results v
        FULL OUTER JOIN fts_results f ON v.user_playbook_id = f.user_playbook_id
    )
    SELECT
        c.user_playbook_id,
        c.user_id,
        c.playbook_name,
        c.request_id,
        c.agent_version,
        c.content,
        c.structured_data,
        c.source,
        c.status,
        c.source_interaction_ids,
        c.created_at,
        c.similarity,
        c.fts_rank,
        c.combined_score
    FROM combined c
    ORDER BY c.combined_score DESC
    LIMIT p_match_count;
END;
$$;

ALTER FUNCTION "public"."hybrid_match_user_playbooks"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_filter_user_id" "text", "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."json_values_as_text"("j" json) RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT
    AS $$
  SELECT coalesce(string_agg(value::text, ' '), '')
  FROM json_each_text(j)
$$;

ALTER FUNCTION "public"."json_values_as_text"("j" json) OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."match_interactions"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer) RETURNS TABLE("interaction_id" bigint, "user_id" "text", "content" "text", "request_id" "text", "created_at" timestamp with time zone, "user_action" "text", "user_action_description" "text", "interacted_image_url" "text", "similarity" double precision)
    LANGUAGE "plpgsql"
    AS $$
begin
    return query
    select
        interactions.interaction_id,
        interactions.user_id,
        interactions.content,
        interactions.request_id,
        interactions.created_at,
        interactions.user_action,
        interactions.user_action_description,
        interactions.interacted_image_url,
        1 - (interactions.embedding <=> query_embedding) as similarity
    from interactions
    where 1 - (interactions.embedding <=> query_embedding) > match_threshold
    order by interactions.embedding <=> query_embedding
    limit match_count;
end;
$$;

ALTER FUNCTION "public"."match_interactions"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer) OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."match_profiles"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer, "current_epoch" bigint) RETURNS TABLE("profile_id" "text", "user_id" "text", "content" "text", "last_modified_timestamp" bigint, "generated_from_request_id" "text", "profile_time_to_live" "text", "expiration_timestamp" bigint, "custom_features" json, "source" character varying, "similarity" double precision)
    LANGUAGE "plpgsql"
    AS $$begin
    return query
    select
        profiles.profile_id,
        profiles.user_id,
        profiles.content,
        profiles.last_modified_timestamp,
        profiles.generated_from_request_id,
        profiles.profile_time_to_live,
        profiles.expiration_timestamp,
        profiles.custom_features,
        profiles.source,
        1 - (profiles.embedding <=> query_embedding) as similarity
    from profiles
    where profiles.expiration_timestamp >= current_epoch
    and 1 - (profiles.embedding <=> query_embedding) > match_threshold
    order by profiles.embedding <=> query_embedding
    limit match_count;
end;$$;

ALTER FUNCTION "public"."match_profiles"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer, "current_epoch" bigint) OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."match_profiles"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer, "current_epoch" bigint, "filter_user_id" "text" DEFAULT NULL::"text", "filter_extractor_name" "text" DEFAULT NULL::"text") RETURNS TABLE("profile_id" "text", "user_id" "text", "content" "text", "last_modified_timestamp" bigint, "generated_from_request_id" "text", "profile_time_to_live" "text", "expiration_timestamp" bigint, "custom_features" json, "source" character varying, "extractor_names" json, "similarity" double precision)
    LANGUAGE "plpgsql"
    AS $$
begin
    return query
    select
        profiles.profile_id,
        profiles.user_id,
        profiles.content,
        profiles.last_modified_timestamp,
        profiles.generated_from_request_id,
        profiles.profile_time_to_live,
        profiles.expiration_timestamp,
        profiles.custom_features,
        profiles.source,
        profiles.extractor_names,
        1 - (profiles.embedding <=> query_embedding) as similarity
    from profiles
    where profiles.expiration_timestamp >= current_epoch
    and 1 - (profiles.embedding <=> query_embedding) > match_threshold
    and (filter_user_id IS NULL OR profiles.user_id = filter_user_id)
    and (filter_extractor_name IS NULL OR profiles.extractor_names::jsonb @> to_jsonb(filter_extractor_name))
    order by profiles.embedding <=> query_embedding
    limit match_count;
end;
$$;

ALTER FUNCTION "public"."match_profiles"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer, "current_epoch" bigint, "filter_user_id" "text", "filter_extractor_name" "text") OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."try_acquire_in_progress_lock"("p_state_key" "text", "p_request_id" "text", "p_stale_lock_seconds" integer DEFAULT 300) RETURNS "jsonb"
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    v_current_state JSONB;
    v_current_time BIGINT;
BEGIN
    v_current_time := EXTRACT(EPOCH FROM NOW())::BIGINT;

    -- Use INSERT ... ON CONFLICT to atomically:
    -- 1. Insert new lock if no row exists
    -- 2. Update existing row based on lock state (stale vs active)
    INSERT INTO _operation_state (service_name, operation_state, updated_at)
    VALUES (
        p_state_key,
        jsonb_build_object(
            'in_progress', true,
            'started_at', v_current_time,
            'current_request_id', p_request_id,
            'pending_request_id', NULL::text
        ),
        NOW()
    )
    ON CONFLICT (service_name) DO UPDATE
    SET operation_state = CASE
        -- Case 1: Not in_progress - acquire lock
        WHEN NOT COALESCE((_operation_state.operation_state->>'in_progress')::boolean, false)
        THEN jsonb_build_object(
            'in_progress', true,
            'started_at', v_current_time,
            'current_request_id', p_request_id,
            'pending_request_id', NULL::text
        )
        -- Case 2: Stale lock (started > stale_lock_seconds ago) - acquire lock
        WHEN (v_current_time - COALESCE((_operation_state.operation_state->>'started_at')::bigint, 0)) >= p_stale_lock_seconds
        THEN jsonb_build_object(
            'in_progress', true,
            'started_at', v_current_time,
            'current_request_id', p_request_id,
            'pending_request_id', NULL::text
        )
        -- Case 3: Active lock - just update pending_request_id
        ELSE jsonb_set(
            _operation_state.operation_state,
            '{pending_request_id}',
            to_jsonb(p_request_id)
        )
    END,
    updated_at = NOW()
    RETURNING operation_state INTO v_current_state;

    -- Return result indicating whether we acquired the lock
    -- We acquired it if current_request_id matches our request_id
    RETURN jsonb_build_object(
        'acquired', (v_current_state->>'current_request_id') = p_request_id,
        'state', v_current_state
    );
END;
$$;

ALTER FUNCTION "public"."try_acquire_in_progress_lock"("p_state_key" "text", "p_request_id" "text", "p_stale_lock_seconds" integer) OWNER TO "postgres";

SET default_tablespace = '';

SET default_table_access_method = "heap";

CREATE TABLE IF NOT EXISTS "public"."_operation_state" (
    "service_name" "text" NOT NULL,
    "operation_state" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);

ALTER TABLE "public"."_operation_state" OWNER TO "postgres";

CREATE TABLE IF NOT EXISTS "public"."agent_playbooks" (
    "agent_playbook_id" bigint NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "content" "text" NOT NULL,
    "playbook_status" "text" NOT NULL,
    "agent_version" "text",
    "playbook_metadata" "text",
    "status" "text",
    "embedding" "public"."vector"(512),
    "playbook_name" "text",
    "structured_data" "jsonb" DEFAULT '{}'::"jsonb",
    "expanded_terms" "text",
    "search_fts" "tsvector" GENERATED ALWAYS AS ("to_tsvector"('"english"'::"regconfig", ((((((COALESCE("content", ''::"text") || ' '::"text") || COALESCE(("structured_data" ->> 'trigger'::"text"), ''::"text")) || ' '::"text") || COALESCE(("structured_data" ->> 'instruction'::"text"), ''::"text")) || ' '::"text") || COALESCE(("structured_data" ->> 'pitfall'::"text"), ''::"text")))) STORED
);

ALTER TABLE "public"."agent_playbooks" OWNER TO "postgres";

CREATE TABLE IF NOT EXISTS "public"."agent_success_evaluation_result" (
    "result_id" bigint NOT NULL,
    "session_id" character varying NOT NULL,
    "created_at" timestamp without time zone DEFAULT "now"() NOT NULL,
    "agent_version" character varying,
    "is_success" boolean,
    "failure_type" character varying,
    "failure_reason" "text",
    "embedding" "public"."vector"(512),
    "regular_vs_shadow" "text",
    "evaluation_name" "text",
    "number_of_correction_per_session" integer DEFAULT 0,
    "user_turns_to_resolution" integer,
    "is_escalated" boolean DEFAULT false
);

ALTER TABLE "public"."agent_success_evaluation_result" OWNER TO "postgres";

ALTER TABLE "public"."agent_success_evaluation_result" ALTER COLUMN "result_id" ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME "public"."agent_success_evaluation_result_result_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

CREATE TABLE IF NOT EXISTS "public"."playbook_aggregation_change_logs" (
    "id" bigint NOT NULL,
    "created_at" integer NOT NULL,
    "playbook_name" "text" NOT NULL,
    "agent_version" "text" NOT NULL,
    "run_mode" "text" NOT NULL,
    "added_feedbacks" "jsonb",
    "removed_feedbacks" "jsonb",
    "updated_feedbacks" "jsonb"
);

ALTER TABLE "public"."playbook_aggregation_change_logs" OWNER TO "postgres";

ALTER TABLE "public"."playbook_aggregation_change_logs" ALTER COLUMN "id" ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME "public"."feedback_aggregation_change_logs_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

ALTER TABLE "public"."agent_playbooks" ALTER COLUMN "agent_playbook_id" ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME "public"."feedbacks_feedback_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

CREATE TABLE IF NOT EXISTS "public"."interactions" (
    "user_id" "text" NOT NULL,
    "content" "text" NOT NULL,
    "shadow_content" "text",
    "request_id" "text",
    "user_action" "text" NOT NULL,
    "user_action_description" "text",
    "interacted_image_url" "text",
    "embedding" "public"."vector"(512),
    "created_at" timestamp with time zone DEFAULT "timezone"('utc'::"text", "now"()) NOT NULL,
    "interaction_id" bigint NOT NULL,
    "role" "text" DEFAULT 'User'::"text" NOT NULL,
    "content_fts" "tsvector" GENERATED ALWAYS AS ("to_tsvector"('"english"'::"regconfig", ((COALESCE("content", ''::"text") || ' '::"text") || COALESCE("user_action_description", ''::"text")))) STORED,
    "tools_used" "jsonb" DEFAULT '[]'::"jsonb",
    "expert_content" "text" DEFAULT ''::"text" NOT NULL
);

ALTER TABLE "public"."interactions" OWNER TO "postgres";

ALTER TABLE "public"."interactions" ALTER COLUMN "interaction_id" ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME "public"."interactions_interaction_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

CREATE TABLE IF NOT EXISTS "public"."profile_change_logs" (
    "id" integer NOT NULL,
    "created_at" integer NOT NULL,
    "user_id" character varying NOT NULL,
    "request_id" character varying NOT NULL,
    "added_profiles" json,
    "removed_profiles" json,
    "mentioned_profiles" json
);

ALTER TABLE "public"."profile_change_logs" OWNER TO "postgres";

CREATE SEQUENCE IF NOT EXISTS "public"."profile_change_logs_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE "public"."profile_change_logs_id_seq" OWNER TO "postgres";

ALTER SEQUENCE "public"."profile_change_logs_id_seq" OWNED BY "public"."profile_change_logs"."id";

CREATE TABLE IF NOT EXISTS "public"."profiles" (
    "user_id" "text" NOT NULL,
    "content" "text" NOT NULL,
    "last_modified_timestamp" bigint NOT NULL,
    "generated_from_request_id" "text",
    "profile_time_to_live" "text" NOT NULL,
    "expiration_timestamp" bigint NOT NULL,
    "custom_features" json,
    "embedding" "public"."vector"(512),
    "created_at" timestamp with time zone DEFAULT "timezone"('utc'::"text", "now"()) NOT NULL,
    "profile_id" "text" NOT NULL,
    "source" character varying,
    "status" "text",
    "extractor_names" json,
    "expanded_terms" "text",
    "content_fts" "tsvector" GENERATED ALWAYS AS ("to_tsvector"('"english"'::"regconfig", ((((COALESCE("content", ''::"text") || ' '::"text") || COALESCE("public"."json_values_as_text"("custom_features"), ''::"text")) || ' '::"text") || COALESCE("expanded_terms", ''::"text")))) STORED,
    "search_fts" "tsvector" GENERATED ALWAYS AS ("to_tsvector"('"english"'::"regconfig", COALESCE("content", ''::"text"))) STORED
);

ALTER TABLE "public"."profiles" OWNER TO "postgres";

CREATE TABLE IF NOT EXISTS "public"."user_playbooks" (
    "user_playbook_id" bigint NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "request_id" "text" NOT NULL,
    "agent_version" "text",
    "content" "text",
    "embedding" "public"."vector"(512),
    "playbook_name" "text",
    "status" "text",
    "source" "text",
    "user_id" "text",
    "source_interaction_ids" bigint[],
    "structured_data" "jsonb" DEFAULT '{}'::"jsonb",
    "expanded_terms" "text",
    "search_fts" "tsvector" GENERATED ALWAYS AS ("to_tsvector"('"english"'::"regconfig", ((((((((COALESCE("content", ''::"text") || ' '::"text") || COALESCE(("structured_data" ->> 'trigger'::"text"), ''::"text")) || ' '::"text") || COALESCE(("structured_data" ->> 'instruction'::"text"), ''::"text")) || ' '::"text") || COALESCE(("structured_data" ->> 'pitfall'::"text"), ''::"text")) || ' '::"text") || COALESCE("source", ''::"text")))) STORED
);

ALTER TABLE "public"."user_playbooks" OWNER TO "postgres";

ALTER TABLE "public"."user_playbooks" ALTER COLUMN "user_playbook_id" ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME "public"."raw_feedbacks_raw_feedback_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

CREATE TABLE IF NOT EXISTS "public"."requests" (
    "request_id" "text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "source" "text",
    "agent_version" "text",
    "user_id" "text" NOT NULL,
    "session_id" "text"
);

ALTER TABLE "public"."requests" OWNER TO "postgres";

CREATE TABLE IF NOT EXISTS "public"."skills" (
    "skill_id" bigint NOT NULL,
    "org_id" "text" NOT NULL,
    "skill_name" "text" NOT NULL,
    "description" "text" DEFAULT ''::"text",
    "version" "text" DEFAULT '1.0.0'::"text",
    "agent_version" "text" DEFAULT ''::"text",
    "playbook_name" "text" DEFAULT ''::"text",
    "instructions" "text" DEFAULT ''::"text",
    "allowed_tools" "jsonb" DEFAULT '[]'::"jsonb",
    "blocking_issues" "jsonb" DEFAULT '[]'::"jsonb",
    "user_playbook_ids" "jsonb" DEFAULT '[]'::"jsonb",
    "skill_status" "text" DEFAULT 'draft'::"text",
    "embedding" "public"."vector"(512),
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"(),
    "content_fts" "tsvector" GENERATED ALWAYS AS ("to_tsvector"('"english"'::"regconfig", ((COALESCE("instructions", ''::"text") || ' '::"text") || COALESCE("description", ''::"text")))) STORED,
    "expanded_terms" "text"
);

ALTER TABLE "public"."skills" OWNER TO "postgres";

ALTER TABLE "public"."skills" ALTER COLUMN "skill_id" ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME "public"."skills_skill_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

ALTER TABLE ONLY "public"."profile_change_logs" ALTER COLUMN "id" SET DEFAULT "nextval"('"public"."profile_change_logs_id_seq"'::"regclass");

ALTER TABLE ONLY "public"."_operation_state"
    ADD CONSTRAINT "_operation_state_pkey" PRIMARY KEY ("service_name");

ALTER TABLE ONLY "public"."_operation_state"
    ADD CONSTRAINT "_operation_state_service_name_key" UNIQUE ("service_name");

ALTER TABLE ONLY "public"."agent_success_evaluation_result"
    ADD CONSTRAINT "agent_success_evaluation_result_pkey" PRIMARY KEY ("result_id");

ALTER TABLE ONLY "public"."agent_success_evaluation_result"
    ADD CONSTRAINT "agent_success_evaluation_result_session_id_eval_name_key" UNIQUE ("session_id", "evaluation_name");

ALTER TABLE ONLY "public"."playbook_aggregation_change_logs"
    ADD CONSTRAINT "feedback_aggregation_change_logs_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."agent_playbooks"
    ADD CONSTRAINT "feedbacks_pkey" PRIMARY KEY ("agent_playbook_id");

ALTER TABLE ONLY "public"."interactions"
    ADD CONSTRAINT "interactions_interaction_id_key" UNIQUE ("interaction_id");

ALTER TABLE ONLY "public"."interactions"
    ADD CONSTRAINT "interactions_pkey" PRIMARY KEY ("interaction_id");

ALTER TABLE ONLY "public"."profile_change_logs"
    ADD CONSTRAINT "profile_change_logs_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."profiles"
    ADD CONSTRAINT "profiles_pkey" PRIMARY KEY ("profile_id");

ALTER TABLE ONLY "public"."user_playbooks"
    ADD CONSTRAINT "raw_feedbacks_pkey" PRIMARY KEY ("user_playbook_id");

ALTER TABLE ONLY "public"."requests"
    ADD CONSTRAINT "requests_pkey" PRIMARY KEY ("request_id");

ALTER TABLE ONLY "public"."skills"
    ADD CONSTRAINT "skills_pkey" PRIMARY KEY ("skill_id");

CREATE INDEX IF NOT EXISTS "idx_agent_playbooks_search_fts" ON "public"."agent_playbooks" USING "gin" ("search_fts");

CREATE INDEX IF NOT EXISTS "idx_feedback_agg_change_logs_created_at" ON "public"."playbook_aggregation_change_logs" USING "btree" ("created_at" DESC);

CREATE INDEX IF NOT EXISTS "idx_feedback_agg_change_logs_name_version" ON "public"."playbook_aggregation_change_logs" USING "btree" ("playbook_name", "agent_version");

CREATE INDEX IF NOT EXISTS "idx_feedbacks_structured_data" ON "public"."agent_playbooks" USING "gin" ("structured_data");

CREATE INDEX IF NOT EXISTS "idx_interactions_content_fts" ON "public"."interactions" USING "gin" ("content_fts");

CREATE INDEX IF NOT EXISTS "idx_interactions_request_id" ON "public"."interactions" USING "btree" ("request_id");

CREATE INDEX IF NOT EXISTS "idx_profiles_content_fts" ON "public"."profiles" USING "gin" ("content_fts");

CREATE INDEX IF NOT EXISTS "idx_profiles_search_fts" ON "public"."profiles" USING "gin" ("search_fts");

CREATE INDEX IF NOT EXISTS "idx_raw_feedbacks_structured_data" ON "public"."user_playbooks" USING "gin" ("structured_data");

CREATE INDEX IF NOT EXISTS "idx_requests_created_at" ON "public"."requests" USING "btree" ("created_at" DESC);

CREATE INDEX IF NOT EXISTS "idx_requests_source" ON "public"."requests" USING "btree" ("source");

CREATE INDEX IF NOT EXISTS "idx_requests_user_id_created_at" ON "public"."requests" USING "btree" ("user_id", "created_at" DESC);

CREATE INDEX IF NOT EXISTS "idx_skills_content_fts" ON "public"."skills" USING "gin" ("content_fts");

CREATE INDEX IF NOT EXISTS "idx_skills_org_agent_version" ON "public"."skills" USING "btree" ("org_id", "agent_version");

CREATE INDEX IF NOT EXISTS "idx_skills_org_feedback_name" ON "public"."skills" USING "btree" ("org_id", "playbook_name");

CREATE INDEX IF NOT EXISTS "idx_skills_org_skill_status" ON "public"."skills" USING "btree" ("org_id", "skill_status");

CREATE INDEX IF NOT EXISTS "idx_user_playbooks_search_fts" ON "public"."user_playbooks" USING "gin" ("search_fts");

CREATE INDEX IF NOT EXISTS "interactions_embedding_idx" ON "public"."interactions" USING "ivfflat" ("embedding" "public"."vector_cosine_ops") WITH ("lists"='100');

CREATE INDEX IF NOT EXISTS "interactions_user_id_idx" ON "public"."interactions" USING "btree" ("user_id");

CREATE INDEX IF NOT EXISTS "ix_profile_change_logs_id" ON "public"."profile_change_logs" USING "btree" ("id");

CREATE INDEX IF NOT EXISTS "ix_profile_change_logs_request_id" ON "public"."profile_change_logs" USING "btree" ("request_id");

CREATE INDEX IF NOT EXISTS "profiles_embedding_idx" ON "public"."profiles" USING "ivfflat" ("embedding" "public"."vector_cosine_ops") WITH ("lists"='100');

CREATE INDEX IF NOT EXISTS "profiles_expiration_timestamp_idx" ON "public"."profiles" USING "btree" ("expiration_timestamp");

CREATE INDEX IF NOT EXISTS "profiles_user_id_idx" ON "public"."profiles" USING "btree" ("user_id");

CREATE INDEX IF NOT EXISTS "raw_feedbacks_user_id_idx" ON "public"."user_playbooks" USING "btree" ("user_id");

CREATE INDEX IF NOT EXISTS "requests_session_id_created_at_idx" ON "public"."requests" USING "btree" ("session_id", "created_at" DESC);

ALTER TABLE ONLY "public"."interactions"
    ADD CONSTRAINT "interactions_request_id_fkey" FOREIGN KEY ("request_id") REFERENCES "public"."requests"("request_id") ON UPDATE CASCADE ON DELETE RESTRICT;

CREATE POLICY "Allow all access to skills" ON "public"."skills" USING (true) WITH CHECK (true);

ALTER TABLE "public"."skills" ENABLE ROW LEVEL SECURITY;

ALTER PUBLICATION "supabase_realtime" OWNER TO "postgres";

GRANT USAGE ON SCHEMA "public" TO "postgres";
GRANT USAGE ON SCHEMA "public" TO "anon";
GRANT USAGE ON SCHEMA "public" TO "authenticated";
GRANT USAGE ON SCHEMA "public" TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_in"("cstring", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_in"("cstring", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_in"("cstring", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_in"("cstring", "oid", integer) TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_out"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_out"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_out"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_out"("public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_recv"("internal", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_recv"("internal", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_recv"("internal", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_recv"("internal", "oid", integer) TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_send"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_send"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_send"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_send"("public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_typmod_in"("cstring"[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_typmod_in"("cstring"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_typmod_in"("cstring"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_typmod_in"("cstring"[]) TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_in"("cstring", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_in"("cstring", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_in"("cstring", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_in"("cstring", "oid", integer) TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_out"("public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_out"("public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_out"("public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_out"("public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_recv"("internal", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_recv"("internal", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_recv"("internal", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_recv"("internal", "oid", integer) TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_send"("public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_send"("public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_send"("public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_send"("public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_typmod_in"("cstring"[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_typmod_in"("cstring"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_typmod_in"("cstring"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_typmod_in"("cstring"[]) TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_in"("cstring", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_in"("cstring", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_in"("cstring", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_in"("cstring", "oid", integer) TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_out"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_out"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_out"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_out"("public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_recv"("internal", "oid", integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_recv"("internal", "oid", integer) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_recv"("internal", "oid", integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_recv"("internal", "oid", integer) TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_send"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_send"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_send"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_send"("public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_typmod_in"("cstring"[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_typmod_in"("cstring"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_typmod_in"("cstring"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_typmod_in"("cstring"[]) TO "service_role";

GRANT ALL ON FUNCTION "public"."array_to_halfvec"(real[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(real[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(real[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(real[], integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(real[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(real[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(real[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(real[], integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."array_to_vector"(real[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_vector"(real[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_vector"(real[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_vector"(real[], integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."array_to_halfvec"(double precision[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(double precision[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(double precision[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(double precision[], integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(double precision[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(double precision[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(double precision[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(double precision[], integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."array_to_vector"(double precision[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_vector"(double precision[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_vector"(double precision[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_vector"(double precision[], integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."array_to_halfvec"(integer[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(integer[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(integer[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(integer[], integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(integer[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(integer[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(integer[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(integer[], integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."array_to_vector"(integer[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_vector"(integer[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_vector"(integer[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_vector"(integer[], integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."array_to_halfvec"(numeric[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(numeric[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(numeric[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_halfvec"(numeric[], integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(numeric[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(numeric[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(numeric[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_sparsevec"(numeric[], integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."array_to_vector"(numeric[], integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."array_to_vector"(numeric[], integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."array_to_vector"(numeric[], integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."array_to_vector"(numeric[], integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_to_float4"("public"."halfvec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_to_float4"("public"."halfvec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_to_float4"("public"."halfvec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_to_float4"("public"."halfvec", integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec"("public"."halfvec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec"("public"."halfvec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec"("public"."halfvec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec"("public"."halfvec", integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_to_sparsevec"("public"."halfvec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_to_sparsevec"("public"."halfvec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_to_sparsevec"("public"."halfvec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_to_sparsevec"("public"."halfvec", integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_to_vector"("public"."halfvec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_to_vector"("public"."halfvec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_to_vector"("public"."halfvec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_to_vector"("public"."halfvec", integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_to_halfvec"("public"."sparsevec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_to_halfvec"("public"."sparsevec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_to_halfvec"("public"."sparsevec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_to_halfvec"("public"."sparsevec", integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec"("public"."sparsevec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec"("public"."sparsevec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec"("public"."sparsevec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec"("public"."sparsevec", integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_to_vector"("public"."sparsevec", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_to_vector"("public"."sparsevec", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_to_vector"("public"."sparsevec", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_to_vector"("public"."sparsevec", integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_to_float4"("public"."vector", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_to_float4"("public"."vector", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_to_float4"("public"."vector", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_to_float4"("public"."vector", integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_to_halfvec"("public"."vector", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_to_halfvec"("public"."vector", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_to_halfvec"("public"."vector", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_to_halfvec"("public"."vector", integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_to_sparsevec"("public"."vector", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_to_sparsevec"("public"."vector", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_to_sparsevec"("public"."vector", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_to_sparsevec"("public"."vector", integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."vector"("public"."vector", integer, boolean) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector"("public"."vector", integer, boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."vector"("public"."vector", integer, boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector"("public"."vector", integer, boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."binary_quantize"("public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."sparsevec", "public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cosine_distance"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."get_last_k_interactions"("p_user_id" "text", "p_limit" integer, "p_sources" "text"[], "p_start_time" bigint, "p_end_time" bigint, "p_agent_version" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."get_last_k_interactions"("p_user_id" "text", "p_limit" integer, "p_sources" "text"[], "p_start_time" bigint, "p_end_time" bigint, "p_agent_version" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_last_k_interactions"("p_user_id" "text", "p_limit" integer, "p_sources" "text"[], "p_start_time" bigint, "p_end_time" bigint, "p_agent_version" "text") TO "service_role";

GRANT ALL ON FUNCTION "public"."get_new_request_interaction_groups"("p_user_id" "text", "p_last_processed_timestamp" timestamp with time zone, "p_excluded_interaction_ids" bigint[], "p_sources" "text"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."get_new_request_interaction_groups"("p_user_id" "text", "p_last_processed_timestamp" timestamp with time zone, "p_excluded_interaction_ids" bigint[], "p_sources" "text"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_new_request_interaction_groups"("p_user_id" "text", "p_last_processed_timestamp" timestamp with time zone, "p_excluded_interaction_ids" bigint[], "p_sources" "text"[]) TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_accum"(double precision[], "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_accum"(double precision[], "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_accum"(double precision[], "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_accum"(double precision[], "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_add"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_add"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_add"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_add"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_avg"(double precision[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_avg"(double precision[]) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_avg"(double precision[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_avg"(double precision[]) TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_cmp"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_cmp"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_cmp"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_cmp"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_combine"(double precision[], double precision[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_combine"(double precision[], double precision[]) TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_combine"(double precision[], double precision[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_combine"(double precision[], double precision[]) TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_concat"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_concat"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_concat"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_concat"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_eq"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_eq"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_eq"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_eq"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_ge"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_ge"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_ge"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_ge"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_gt"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_gt"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_gt"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_gt"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_l2_squared_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_l2_squared_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_l2_squared_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_l2_squared_distance"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_le"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_le"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_le"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_le"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_lt"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_lt"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_lt"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_lt"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_mul"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_mul"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_mul"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_mul"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_ne"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_ne"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_ne"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_ne"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_negative_inner_product"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_negative_inner_product"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_negative_inner_product"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_negative_inner_product"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_spherical_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_spherical_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_spherical_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_spherical_distance"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."halfvec_sub"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."halfvec_sub"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."halfvec_sub"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."halfvec_sub"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."hamming_distance"(bit, bit) TO "postgres";
GRANT ALL ON FUNCTION "public"."hamming_distance"(bit, bit) TO "anon";
GRANT ALL ON FUNCTION "public"."hamming_distance"(bit, bit) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hamming_distance"(bit, bit) TO "service_role";

GRANT ALL ON FUNCTION "public"."hnsw_bit_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."hnsw_bit_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."hnsw_bit_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."hnsw_bit_support"("internal") TO "service_role";

GRANT ALL ON FUNCTION "public"."hnsw_halfvec_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."hnsw_halfvec_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."hnsw_halfvec_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."hnsw_halfvec_support"("internal") TO "service_role";

GRANT ALL ON FUNCTION "public"."hnsw_sparsevec_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."hnsw_sparsevec_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."hnsw_sparsevec_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."hnsw_sparsevec_support"("internal") TO "service_role";

GRANT ALL ON FUNCTION "public"."hnswhandler"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."hnswhandler"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."hnswhandler"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."hnswhandler"("internal") TO "service_role";

GRANT ALL ON FUNCTION "public"."hybrid_match_agent_playbooks"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) TO "anon";
GRANT ALL ON FUNCTION "public"."hybrid_match_agent_playbooks"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hybrid_match_agent_playbooks"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) TO "service_role";

GRANT ALL ON FUNCTION "public"."hybrid_match_interactions"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) TO "anon";
GRANT ALL ON FUNCTION "public"."hybrid_match_interactions"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hybrid_match_interactions"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) TO "service_role";

GRANT ALL ON FUNCTION "public"."hybrid_match_profiles"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_current_epoch" bigint, "p_filter_user_id" "text", "p_search_mode" "text", "p_rrf_k" integer, "p_filter_extractor_name" "text", "p_vector_weight" double precision, "p_fts_weight" double precision) TO "anon";
GRANT ALL ON FUNCTION "public"."hybrid_match_profiles"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_current_epoch" bigint, "p_filter_user_id" "text", "p_search_mode" "text", "p_rrf_k" integer, "p_filter_extractor_name" "text", "p_vector_weight" double precision, "p_fts_weight" double precision) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hybrid_match_profiles"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_current_epoch" bigint, "p_filter_user_id" "text", "p_search_mode" "text", "p_rrf_k" integer, "p_filter_extractor_name" "text", "p_vector_weight" double precision, "p_fts_weight" double precision) TO "service_role";

GRANT ALL ON FUNCTION "public"."hybrid_match_skills"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_org_id" "text", "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) TO "anon";
GRANT ALL ON FUNCTION "public"."hybrid_match_skills"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_org_id" "text", "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hybrid_match_skills"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_org_id" "text", "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) TO "service_role";

GRANT ALL ON FUNCTION "public"."hybrid_match_user_playbooks"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_filter_user_id" "text", "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) TO "anon";
GRANT ALL ON FUNCTION "public"."hybrid_match_user_playbooks"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_filter_user_id" "text", "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hybrid_match_user_playbooks"("p_query_embedding" "public"."vector", "p_query_text" "text", "p_match_threshold" double precision, "p_match_count" integer, "p_filter_user_id" "text", "p_search_mode" "text", "p_rrf_k" integer, "p_vector_weight" double precision, "p_fts_weight" double precision) TO "service_role";

GRANT ALL ON FUNCTION "public"."inner_product"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."inner_product"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."sparsevec", "public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."inner_product"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."inner_product"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."ivfflat_bit_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."ivfflat_bit_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."ivfflat_bit_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."ivfflat_bit_support"("internal") TO "service_role";

GRANT ALL ON FUNCTION "public"."ivfflat_halfvec_support"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."ivfflat_halfvec_support"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."ivfflat_halfvec_support"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."ivfflat_halfvec_support"("internal") TO "service_role";

GRANT ALL ON FUNCTION "public"."ivfflathandler"("internal") TO "postgres";
GRANT ALL ON FUNCTION "public"."ivfflathandler"("internal") TO "anon";
GRANT ALL ON FUNCTION "public"."ivfflathandler"("internal") TO "authenticated";
GRANT ALL ON FUNCTION "public"."ivfflathandler"("internal") TO "service_role";

GRANT ALL ON FUNCTION "public"."jaccard_distance"(bit, bit) TO "postgres";
GRANT ALL ON FUNCTION "public"."jaccard_distance"(bit, bit) TO "anon";
GRANT ALL ON FUNCTION "public"."jaccard_distance"(bit, bit) TO "authenticated";
GRANT ALL ON FUNCTION "public"."jaccard_distance"(bit, bit) TO "service_role";

GRANT ALL ON FUNCTION "public"."json_values_as_text"("j" json) TO "anon";
GRANT ALL ON FUNCTION "public"."json_values_as_text"("j" json) TO "authenticated";
GRANT ALL ON FUNCTION "public"."json_values_as_text"("j" json) TO "service_role";

GRANT ALL ON FUNCTION "public"."l1_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."l1_distance"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."sparsevec", "public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."l1_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l1_distance"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."l2_distance"("public"."halfvec", "public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."halfvec", "public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."halfvec", "public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."halfvec", "public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."l2_distance"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."sparsevec", "public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."l2_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_distance"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."l2_norm"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."l2_norm"("public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_norm"("public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."l2_normalize"("public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."match_interactions"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."match_interactions"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."match_interactions"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer) TO "service_role";

GRANT ALL ON FUNCTION "public"."match_profiles"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer, "current_epoch" bigint) TO "anon";
GRANT ALL ON FUNCTION "public"."match_profiles"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer, "current_epoch" bigint) TO "authenticated";
GRANT ALL ON FUNCTION "public"."match_profiles"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer, "current_epoch" bigint) TO "service_role";

GRANT ALL ON FUNCTION "public"."match_profiles"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer, "current_epoch" bigint, "filter_user_id" "text", "filter_extractor_name" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."match_profiles"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer, "current_epoch" bigint, "filter_user_id" "text", "filter_extractor_name" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."match_profiles"("query_embedding" "public"."vector", "match_threshold" double precision, "match_count" integer, "current_epoch" bigint, "filter_user_id" "text", "filter_extractor_name" "text") TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_cmp"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_cmp"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_cmp"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_cmp"("public"."sparsevec", "public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_eq"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_eq"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_eq"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_eq"("public"."sparsevec", "public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_ge"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_ge"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_ge"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_ge"("public"."sparsevec", "public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_gt"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_gt"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_gt"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_gt"("public"."sparsevec", "public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_l2_squared_distance"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_l2_squared_distance"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_l2_squared_distance"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_l2_squared_distance"("public"."sparsevec", "public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_le"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_le"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_le"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_le"("public"."sparsevec", "public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_lt"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_lt"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_lt"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_lt"("public"."sparsevec", "public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_ne"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_ne"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_ne"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_ne"("public"."sparsevec", "public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."sparsevec_negative_inner_product"("public"."sparsevec", "public"."sparsevec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sparsevec_negative_inner_product"("public"."sparsevec", "public"."sparsevec") TO "anon";
GRANT ALL ON FUNCTION "public"."sparsevec_negative_inner_product"("public"."sparsevec", "public"."sparsevec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sparsevec_negative_inner_product"("public"."sparsevec", "public"."sparsevec") TO "service_role";

GRANT ALL ON FUNCTION "public"."subvector"("public"."halfvec", integer, integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."subvector"("public"."halfvec", integer, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."subvector"("public"."halfvec", integer, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."subvector"("public"."halfvec", integer, integer) TO "service_role";

GRANT ALL ON FUNCTION "public"."subvector"("public"."vector", integer, integer) TO "postgres";
GRANT ALL ON FUNCTION "public"."subvector"("public"."vector", integer, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."subvector"("public"."vector", integer, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."subvector"("public"."vector", integer, integer) TO "service_role";

GRANT ALL ON FUNCTION "public"."try_acquire_in_progress_lock"("p_state_key" "text", "p_request_id" "text", "p_stale_lock_seconds" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."try_acquire_in_progress_lock"("p_state_key" "text", "p_request_id" "text", "p_stale_lock_seconds" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."try_acquire_in_progress_lock"("p_state_key" "text", "p_request_id" "text", "p_stale_lock_seconds" integer) TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_accum"(double precision[], "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_accum"(double precision[], "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_accum"(double precision[], "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_accum"(double precision[], "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_add"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_add"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_add"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_add"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_avg"(double precision[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_avg"(double precision[]) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_avg"(double precision[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_avg"(double precision[]) TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_cmp"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_cmp"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_cmp"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_cmp"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_combine"(double precision[], double precision[]) TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_combine"(double precision[], double precision[]) TO "anon";
GRANT ALL ON FUNCTION "public"."vector_combine"(double precision[], double precision[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_combine"(double precision[], double precision[]) TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_concat"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_concat"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_concat"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_concat"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_dims"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_dims"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_dims"("public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_eq"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_eq"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_eq"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_eq"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_ge"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_ge"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_ge"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_ge"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_gt"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_gt"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_gt"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_gt"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_l2_squared_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_l2_squared_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_l2_squared_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_l2_squared_distance"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_le"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_le"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_le"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_le"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_lt"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_lt"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_lt"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_lt"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_mul"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_mul"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_mul"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_mul"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_ne"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_ne"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_ne"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_ne"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_negative_inner_product"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_negative_inner_product"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_negative_inner_product"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_negative_inner_product"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_norm"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_norm"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_norm"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_norm"("public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_spherical_distance"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_spherical_distance"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_spherical_distance"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_spherical_distance"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."vector_sub"("public"."vector", "public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."vector_sub"("public"."vector", "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."vector_sub"("public"."vector", "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."vector_sub"("public"."vector", "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."avg"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."avg"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."avg"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."avg"("public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."avg"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."avg"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."avg"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."avg"("public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."sum"("public"."halfvec") TO "postgres";
GRANT ALL ON FUNCTION "public"."sum"("public"."halfvec") TO "anon";
GRANT ALL ON FUNCTION "public"."sum"("public"."halfvec") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sum"("public"."halfvec") TO "service_role";

GRANT ALL ON FUNCTION "public"."sum"("public"."vector") TO "postgres";
GRANT ALL ON FUNCTION "public"."sum"("public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."sum"("public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."sum"("public"."vector") TO "service_role";

GRANT ALL ON TABLE "public"."_operation_state" TO "anon";
GRANT ALL ON TABLE "public"."_operation_state" TO "authenticated";
GRANT ALL ON TABLE "public"."_operation_state" TO "service_role";

GRANT ALL ON TABLE "public"."agent_playbooks" TO "anon";
GRANT ALL ON TABLE "public"."agent_playbooks" TO "authenticated";
GRANT ALL ON TABLE "public"."agent_playbooks" TO "service_role";

GRANT ALL ON TABLE "public"."agent_success_evaluation_result" TO "anon";
GRANT ALL ON TABLE "public"."agent_success_evaluation_result" TO "authenticated";
GRANT ALL ON TABLE "public"."agent_success_evaluation_result" TO "service_role";

GRANT ALL ON SEQUENCE "public"."agent_success_evaluation_result_result_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."agent_success_evaluation_result_result_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."agent_success_evaluation_result_result_id_seq" TO "service_role";

GRANT ALL ON TABLE "public"."playbook_aggregation_change_logs" TO "anon";
GRANT ALL ON TABLE "public"."playbook_aggregation_change_logs" TO "authenticated";
GRANT ALL ON TABLE "public"."playbook_aggregation_change_logs" TO "service_role";

GRANT ALL ON SEQUENCE "public"."feedback_aggregation_change_logs_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."feedback_aggregation_change_logs_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."feedback_aggregation_change_logs_id_seq" TO "service_role";

GRANT ALL ON SEQUENCE "public"."feedbacks_feedback_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."feedbacks_feedback_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."feedbacks_feedback_id_seq" TO "service_role";

GRANT ALL ON TABLE "public"."interactions" TO "anon";
GRANT ALL ON TABLE "public"."interactions" TO "authenticated";
GRANT ALL ON TABLE "public"."interactions" TO "service_role";

GRANT ALL ON SEQUENCE "public"."interactions_interaction_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."interactions_interaction_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."interactions_interaction_id_seq" TO "service_role";

GRANT ALL ON TABLE "public"."profile_change_logs" TO "anon";
GRANT ALL ON TABLE "public"."profile_change_logs" TO "authenticated";
GRANT ALL ON TABLE "public"."profile_change_logs" TO "service_role";

GRANT ALL ON SEQUENCE "public"."profile_change_logs_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."profile_change_logs_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."profile_change_logs_id_seq" TO "service_role";

GRANT ALL ON TABLE "public"."profiles" TO "anon";
GRANT ALL ON TABLE "public"."profiles" TO "authenticated";
GRANT ALL ON TABLE "public"."profiles" TO "service_role";

GRANT ALL ON TABLE "public"."user_playbooks" TO "anon";
GRANT ALL ON TABLE "public"."user_playbooks" TO "authenticated";
GRANT ALL ON TABLE "public"."user_playbooks" TO "service_role";

GRANT ALL ON SEQUENCE "public"."raw_feedbacks_raw_feedback_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."raw_feedbacks_raw_feedback_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."raw_feedbacks_raw_feedback_id_seq" TO "service_role";

GRANT ALL ON TABLE "public"."requests" TO "anon";
GRANT ALL ON TABLE "public"."requests" TO "authenticated";
GRANT ALL ON TABLE "public"."requests" TO "service_role";

GRANT ALL ON TABLE "public"."skills" TO "anon";
GRANT ALL ON TABLE "public"."skills" TO "authenticated";
GRANT ALL ON TABLE "public"."skills" TO "service_role";

GRANT ALL ON SEQUENCE "public"."skills_skill_id_seq" TO "anon";
GRANT ALL ON SEQUENCE "public"."skills_skill_id_seq" TO "authenticated";
GRANT ALL ON SEQUENCE "public"."skills_skill_id_seq" TO "service_role";

ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "service_role";

ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "service_role";

ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "service_role";

--
-- Dumped schema changes for auth and storage
--
