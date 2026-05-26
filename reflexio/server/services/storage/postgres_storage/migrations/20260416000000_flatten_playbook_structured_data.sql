-- Migration: Flatten playbook schema
-- Moves trigger, rationale, blocking_issue from nested structured_data JSONB
-- to top-level columns on agent_playbooks and user_playbooks.
-- Removes instruction, pitfall, embedding_text fields (no longer used).

-- ============================================================
-- 1. Add new columns
-- ============================================================

ALTER TABLE public.agent_playbooks ADD COLUMN IF NOT EXISTS "trigger" TEXT;
ALTER TABLE public.agent_playbooks ADD COLUMN IF NOT EXISTS rationale TEXT;
ALTER TABLE public.agent_playbooks ADD COLUMN IF NOT EXISTS blocking_issue JSONB;

ALTER TABLE public.user_playbooks ADD COLUMN IF NOT EXISTS "trigger" TEXT;
ALTER TABLE public.user_playbooks ADD COLUMN IF NOT EXISTS rationale TEXT;
ALTER TABLE public.user_playbooks ADD COLUMN IF NOT EXISTS blocking_issue JSONB;

-- ============================================================
-- 2. Migrate existing data from structured_data JSONB
--    (only runs if structured_data column still exists)
-- ============================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'agent_playbooks' AND column_name = 'structured_data'
    ) THEN
        UPDATE public.agent_playbooks
        SET "trigger"      = structured_data ->> 'trigger',
            rationale      = structured_data ->> 'rationale',
            blocking_issue = structured_data -> 'blocking_issue'
        WHERE structured_data IS NOT NULL
          AND structured_data != '{}'::jsonb;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'user_playbooks' AND column_name = 'structured_data'
    ) THEN
        UPDATE public.user_playbooks
        SET "trigger"      = structured_data ->> 'trigger',
            rationale      = structured_data ->> 'rationale',
            blocking_issue = structured_data -> 'blocking_issue'
        WHERE structured_data IS NOT NULL
          AND structured_data != '{}'::jsonb;
    END IF;
END $$;

-- ============================================================
-- 3. Drop + recreate search_fts generated column
--    (PostgreSQL does not allow ALTER on generated columns)
-- ============================================================

-- agent_playbooks: FTS from content + trigger
ALTER TABLE public.agent_playbooks DROP COLUMN IF EXISTS search_fts;
ALTER TABLE public.agent_playbooks ADD COLUMN search_fts tsvector
    GENERATED ALWAYS AS (
        to_tsvector('english',
            COALESCE(content, '') || ' ' || COALESCE("trigger", '')
        )
    ) STORED;

-- user_playbooks: FTS from content + trigger + source
ALTER TABLE public.user_playbooks DROP COLUMN IF EXISTS search_fts;
ALTER TABLE public.user_playbooks ADD COLUMN search_fts tsvector
    GENERATED ALWAYS AS (
        to_tsvector('english',
            COALESCE(content, '') || ' ' || COALESCE("trigger", '') || ' ' || COALESCE(source, '')
        )
    ) STORED;

-- ============================================================
-- 4. Drop old GIN indexes on structured_data, recreate FTS indexes
-- ============================================================

DROP INDEX IF EXISTS public.idx_feedbacks_structured_data;
DROP INDEX IF EXISTS public.idx_raw_feedbacks_structured_data;

CREATE INDEX IF NOT EXISTS idx_agent_playbooks_search_fts ON public.agent_playbooks USING gin (search_fts);
CREATE INDEX IF NOT EXISTS idx_user_playbooks_search_fts ON public.user_playbooks USING gin (search_fts);

-- ============================================================
-- 5. Drop the structured_data column
-- ============================================================

ALTER TABLE public.agent_playbooks DROP COLUMN IF EXISTS structured_data;
ALTER TABLE public.user_playbooks DROP COLUMN IF EXISTS structured_data;

-- ============================================================
-- 6. Replace hybrid search RPC functions
--    (return type changes: structured_data -> trigger, rationale, blocking_issue)
-- ============================================================

-- Drop old functions (signature change requires explicit drop). The argument
-- types must be fully qualified — the init migration created these functions
-- with ``public.vector`` as the embedding argument, and Postgres only matches
-- DROP FUNCTION when the signature is byte-for-byte identical. Using bare
-- ``vector`` here resolves via search_path on ``public`` (so the legacy
-- in-place migration on the public schema works), but for per-org schemas
-- rendered by ``render_migration_sql_for_schema`` the search path didn't
-- include ``public`` early enough and the DROP became a no-op — leaving the
-- old function in place so the subsequent CREATE OR REPLACE tripped over
-- "cannot change return type". Qualifying the type to ``public.vector``
-- (which the renderer leaves untouched) makes the signature unambiguous.
DROP FUNCTION IF EXISTS public.hybrid_match_agent_playbooks(public.vector, text, double precision, integer, text, integer, double precision, double precision);
DROP FUNCTION IF EXISTS public.hybrid_match_user_playbooks(public.vector, text, double precision, integer, text, text, integer, double precision, double precision);

-- hybrid_match_agent_playbooks
CREATE OR REPLACE FUNCTION public.hybrid_match_agent_playbooks(
    p_query_embedding public.vector,
    p_query_text text,
    p_match_threshold double precision DEFAULT 0.7,
    p_match_count integer DEFAULT 10,
    p_search_mode text DEFAULT 'hybrid',
    p_rrf_k integer DEFAULT 60,
    p_vector_weight double precision DEFAULT 1.0,
    p_fts_weight double precision DEFAULT 1.0
)
RETURNS TABLE(
    agent_playbook_id bigint,
    playbook_name text,
    content text,
    "trigger" text,
    rationale text,
    blocking_issue jsonb,
    playbook_status text,
    agent_version text,
    playbook_metadata text,
    created_at timestamp with time zone,
    status text,
    similarity double precision,
    fts_rank double precision,
    combined_score double precision
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
            ap.agent_playbook_id,
            ap.playbook_name,
            ap.content,
            ap."trigger",
            ap.rationale,
            ap.blocking_issue,
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
            ap."trigger",
            ap.rationale,
            ap.blocking_issue,
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
            COALESCE(v."trigger", f."trigger") as "trigger",
            COALESCE(v.rationale, f.rationale) as rationale,
            COALESCE(v.blocking_issue, f.blocking_issue) as blocking_issue,
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
        c."trigger",
        c.rationale,
        c.blocking_issue,
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

ALTER FUNCTION public.hybrid_match_agent_playbooks(public.vector, text, double precision, integer, text, integer, double precision, double precision) OWNER TO postgres;

GRANT ALL ON FUNCTION public.hybrid_match_agent_playbooks(public.vector, text, double precision, integer, text, integer, double precision, double precision) TO anon;
GRANT ALL ON FUNCTION public.hybrid_match_agent_playbooks(public.vector, text, double precision, integer, text, integer, double precision, double precision) TO authenticated;
GRANT ALL ON FUNCTION public.hybrid_match_agent_playbooks(public.vector, text, double precision, integer, text, integer, double precision, double precision) TO service_role;


-- hybrid_match_user_playbooks
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
    combined_score double precision
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
        c.combined_score
    FROM combined c
    ORDER BY c.combined_score DESC
    LIMIT p_match_count;
END;
$$;

ALTER FUNCTION public.hybrid_match_user_playbooks(public.vector, text, double precision, integer, text, text, integer, double precision, double precision) OWNER TO postgres;

GRANT ALL ON FUNCTION public.hybrid_match_user_playbooks(public.vector, text, double precision, integer, text, text, integer, double precision, double precision) TO anon;
GRANT ALL ON FUNCTION public.hybrid_match_user_playbooks(public.vector, text, double precision, integer, text, text, integer, double precision, double precision) TO authenticated;
GRANT ALL ON FUNCTION public.hybrid_match_user_playbooks(public.vector, text, double precision, integer, text, text, integer, double precision, double precision) TO service_role;
