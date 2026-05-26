-- Mirrors the SQLite migration in open_source/reflexio/.../sqlite_storage/_base.py
-- (_migrate_agentic_signals). Adds three nullable TEXT columns to public.profiles
-- and public.user_playbooks so agentic extraction can persist source_span,
-- free-form notes, and the reader angle that produced each item. Columns are
-- additive and nullable — classic extraction pipelines leave them NULL.
ALTER TABLE public.profiles       ADD COLUMN IF NOT EXISTS source_span  TEXT;
ALTER TABLE public.profiles       ADD COLUMN IF NOT EXISTS notes        TEXT;
ALTER TABLE public.profiles       ADD COLUMN IF NOT EXISTS reader_angle TEXT;

ALTER TABLE public.user_playbooks ADD COLUMN IF NOT EXISTS source_span  TEXT;
ALTER TABLE public.user_playbooks ADD COLUMN IF NOT EXISTS notes        TEXT;
ALTER TABLE public.user_playbooks ADD COLUMN IF NOT EXISTS reader_angle TEXT;

-- ============================================================
-- Extend hybrid-search RPC return types to include source_span,
-- notes, and reader_angle.
--
-- Postgres does not allow RETURNS TABLE to be changed on an
-- existing function via CREATE OR REPLACE FUNCTION — it raises
-- "cannot change return type of existing function". The only
-- safe path is DROP (full signature) then CREATE OR REPLACE.
-- This pattern is used elsewhere in this project; see the DROP
-- block in 20260416000000_flatten_playbook_structured_data.sql
-- around line 96.
-- ============================================================

-- hybrid_match_profiles
-- Drop the existing function by its full signature (from init_data_schema.sql line 392).
DROP FUNCTION IF EXISTS public.hybrid_match_profiles(
    public.vector, text, double precision, integer, bigint,
    text, text, integer, text, double precision, double precision
);

CREATE OR REPLACE FUNCTION public.hybrid_match_profiles(
    p_query_embedding public.vector,
    p_query_text text,
    p_match_threshold double precision DEFAULT 0.3,
    p_match_count integer DEFAULT 10,
    p_current_epoch bigint DEFAULT 0,
    p_filter_user_id text DEFAULT NULL,
    p_search_mode text DEFAULT 'hybrid',
    p_rrf_k integer DEFAULT 60,
    p_filter_extractor_name text DEFAULT NULL,
    p_vector_weight double precision DEFAULT 1.0,
    p_fts_weight double precision DEFAULT 1.0
)
RETURNS TABLE(
    profile_id text,
    user_id text,
    content text,
    last_modified_timestamp bigint,
    generated_from_request_id text,
    profile_time_to_live text,
    expiration_timestamp bigint,
    custom_features json,
    source character varying,
    status text,
    extractor_names json,
    similarity double precision,
    fts_rank double precision,
    combined_score double precision,
    source_span text,
    notes text,
    reader_angle text
)
LANGUAGE plpgsql
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
            p.source_span,
            p.notes,
            p.reader_angle,
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
            p.source_span,
            p.notes,
            p.reader_angle,
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
            COALESCE(v.source_span, f.source_span) as source_span,
            COALESCE(v.notes, f.notes) as notes,
            COALESCE(v.reader_angle, f.reader_angle) as reader_angle,
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
        c.combined_score,
        c.source_span,
        c.notes,
        c.reader_angle
    FROM combined c
    ORDER BY c.combined_score DESC
    LIMIT p_match_count;
END;
$$;

ALTER FUNCTION public.hybrid_match_profiles(
    public.vector, text, double precision, integer, bigint,
    text, text, integer, text, double precision, double precision
) OWNER TO postgres;

GRANT ALL ON FUNCTION public.hybrid_match_profiles(
    public.vector, text, double precision, integer, bigint,
    text, text, integer, text, double precision, double precision
) TO anon;
GRANT ALL ON FUNCTION public.hybrid_match_profiles(
    public.vector, text, double precision, integer, bigint,
    text, text, integer, text, double precision, double precision
) TO authenticated;
GRANT ALL ON FUNCTION public.hybrid_match_profiles(
    public.vector, text, double precision, integer, bigint,
    text, text, integer, text, double precision, double precision
) TO service_role;

-- hybrid_match_user_playbooks
-- Drop the existing function by its full signature (from
-- 20260416000000_flatten_playbook_structured_data.sql line 367).
DROP FUNCTION IF EXISTS public.hybrid_match_user_playbooks(
    public.vector, text, double precision, integer,
    text, text, integer, double precision, double precision
);

CREATE OR REPLACE FUNCTION public.hybrid_match_user_playbooks(
    p_query_embedding public.vector,
    p_query_text text,
    p_match_threshold double precision DEFAULT 0.7,
    p_match_count integer DEFAULT 10,
    p_filter_user_id text DEFAULT NULL,
    p_search_mode text DEFAULT 'hybrid',
    p_rrf_k integer DEFAULT 60,
    p_vector_weight double precision DEFAULT 1.0,
    p_fts_weight double precision DEFAULT 1.0
)
RETURNS TABLE(
    user_playbook_id bigint,
    user_id text,
    playbook_name text,
    request_id text,
    agent_version text,
    content text,
    "trigger" text,
    rationale text,
    blocking_issue jsonb,
    source text,
    status text,
    source_interaction_ids bigint[],
    created_at timestamp with time zone,
    similarity double precision,
    fts_rank double precision,
    combined_score double precision,
    source_span text,
    notes text,
    reader_angle text
)
LANGUAGE plpgsql
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
            up."trigger",
            up.rationale,
            up.blocking_issue,
            up.source,
            up.status,
            up.source_interaction_ids,
            up.created_at,
            up.source_span,
            up.notes,
            up.reader_angle,
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
            up."trigger",
            up.rationale,
            up.blocking_issue,
            up.source,
            up.status,
            up.source_interaction_ids,
            up.created_at,
            up.source_span,
            up.notes,
            up.reader_angle,
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
            COALESCE(v."trigger", f."trigger") as "trigger",
            COALESCE(v.rationale, f.rationale) as rationale,
            COALESCE(v.blocking_issue, f.blocking_issue) as blocking_issue,
            COALESCE(v.source, f.source) as source,
            COALESCE(v.status, f.status) as status,
            COALESCE(v.source_interaction_ids, f.source_interaction_ids) as source_interaction_ids,
            COALESCE(v.created_at, f.created_at) as created_at,
            COALESCE(v.source_span, f.source_span) as source_span,
            COALESCE(v.notes, f.notes) as notes,
            COALESCE(v.reader_angle, f.reader_angle) as reader_angle,
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
        c."trigger",
        c.rationale,
        c.blocking_issue,
        c.source,
        c.status,
        c.source_interaction_ids,
        c.created_at,
        c.similarity,
        c.fts_rank,
        c.combined_score,
        c.source_span,
        c.notes,
        c.reader_angle
    FROM combined c
    ORDER BY c.combined_score DESC
    LIMIT p_match_count;
END;
$$;

ALTER FUNCTION public.hybrid_match_user_playbooks(
    public.vector, text, double precision, integer,
    text, text, integer, double precision, double precision
) OWNER TO postgres;

GRANT ALL ON FUNCTION public.hybrid_match_user_playbooks(
    public.vector, text, double precision, integer,
    text, text, integer, double precision, double precision
) TO anon;
GRANT ALL ON FUNCTION public.hybrid_match_user_playbooks(
    public.vector, text, double precision, integer,
    text, text, integer, double precision, double precision
) TO authenticated;
GRANT ALL ON FUNCTION public.hybrid_match_user_playbooks(
    public.vector, text, double precision, integer,
    text, text, integer, double precision, double precision
) TO service_role;
