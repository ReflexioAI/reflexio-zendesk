-- Durable incremental playbook aggregation for native PostgreSQL storage.

-- Durable, bounded incremental user-playbook aggregation and queue indexes.


CREATE TABLE IF NOT EXISTS public.playbook_aggregation_state (
    agent_version text PRIMARY KEY CHECK (btrim(agent_version) <> ''),
    last_success_at bigint,
    pending boolean NOT NULL DEFAULT true,
    next_attempt_at bigint NOT NULL DEFAULT floor(extract(epoch FROM clock_timestamp()))::bigint,
    state_version bigint NOT NULL DEFAULT 0,
    retry_cursor bigint NOT NULL DEFAULT 0,
    bootstrap_status text NOT NULL DEFAULT 'pending'
);
CREATE INDEX IF NOT EXISTS idx_playbook_aggregation_state_due
    ON public.playbook_aggregation_state(pending, next_attempt_at, agent_version);

CREATE TABLE IF NOT EXISTS public.playbook_aggregation_lease (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    claim_owner text,
    claim_fence bigint NOT NULL DEFAULT 0,
    claim_expires_at bigint,
    agent_version text
);
INSERT INTO public.playbook_aggregation_lease(singleton) VALUES (true)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS public.playbook_aggregation_cluster (
    cluster_id uuid PRIMARY KEY,
    agent_version text NOT NULL,
    agent_playbook_id bigint REFERENCES public.agent_playbooks(agent_playbook_id),
    centroid public.vector(512),
    vector_sum public.vector(512),
    member_count integer NOT NULL DEFAULT 0 CHECK (member_count >= 0),
    embedding_model text,
    embedding_dimension integer,
    normalization text NOT NULL DEFAULT 'l2',
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'rebuilding')),
    dirty boolean NOT NULL DEFAULT false,
    rebuild_cursor bigint NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_playbook_aggregation_cluster_version
    ON public.playbook_aggregation_cluster(agent_version, state, cluster_id);
CREATE INDEX IF NOT EXISTS idx_playbook_aggregation_cluster_centroid
    ON public.playbook_aggregation_cluster
    USING hnsw (centroid public.vector_cosine_ops);

CREATE TABLE IF NOT EXISTS public.playbook_aggregation_item (
    agent_version text NOT NULL,
    user_playbook_id bigint NOT NULL,
    disposition text NOT NULL CHECK (
        disposition IN ('residual', 'cluster_member', 'terminal_noop')
    ),
    cluster_id uuid REFERENCES public.playbook_aggregation_cluster(cluster_id),
    reason text,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_attempt_at bigint,
    created_at bigint NOT NULL DEFAULT floor(extract(epoch FROM clock_timestamp()))::bigint,
    updated_at bigint NOT NULL DEFAULT floor(extract(epoch FROM clock_timestamp()))::bigint,
    PRIMARY KEY (agent_version, user_playbook_id)
);
CREATE INDEX IF NOT EXISTS idx_playbook_aggregation_item_residual
    ON public.playbook_aggregation_item(
        agent_version, disposition, attempt_count, last_attempt_at, user_playbook_id
    );

CREATE TABLE IF NOT EXISTS public.playbook_aggregation_invalidation (
    invalidation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    agent_version text NOT NULL,
    operation text NOT NULL CHECK (operation IN (
        'create', 'merge', 'revise', 'status_change', 'archive', 'hard_delete', 'purge'
    )),
    entity_id bigint NOT NULL,
    source_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at bigint NOT NULL DEFAULT floor(extract(epoch FROM clock_timestamp()))::bigint,
    processed_at bigint
);
CREATE INDEX IF NOT EXISTS idx_playbook_aggregation_invalidation_pending
    ON public.playbook_aggregation_invalidation(agent_version, invalidation_id)
    WHERE processed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_playbook_aggregation_invalidation_retention
    ON public.playbook_aggregation_invalidation(processed_at)
    WHERE processed_at IS NOT NULL;


CREATE OR REPLACE FUNCTION public.capture_playbook_aggregation_invalidation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = ''
AS $$
DECLARE
    v_agent_version text;
    v_candidate text;
BEGIN
    IF NEW.entity_type <> 'user_playbook' OR NEW.op NOT IN (
        'create', 'merge', 'revise', 'status_change', 'purge'
    ) THEN
        RETURN NEW;
    END IF;
    IF NEW.entity_id ~ '^[0-9]+$' THEN
        SELECT p.agent_version INTO v_agent_version
        FROM public.user_playbooks p
        WHERE p.user_playbook_id = NEW.entity_id::bigint;
    END IF;
    IF v_agent_version IS NULL THEN
        FOR v_candidate IN SELECT jsonb_array_elements_text(NEW.source_ids) LOOP
            IF v_candidate ~ '^[0-9]+$' THEN
                SELECT p.agent_version INTO v_agent_version
                FROM public.user_playbooks p
                WHERE p.user_playbook_id = v_candidate::bigint;
            END IF;
            EXIT WHEN v_agent_version IS NOT NULL;
        END LOOP;
    END IF;
    IF NULLIF(btrim(v_agent_version), '') IS NULL THEN
        RETURN NEW;
    END IF;
    IF NEW.entity_id !~ '^[0-9]+$' THEN
        RETURN NEW;
    END IF;
    INSERT INTO public.playbook_aggregation_invalidation(
        agent_version, operation, entity_id, source_ids
    ) VALUES (
        v_agent_version, NEW.op, NEW.entity_id::bigint, NEW.source_ids
    );
    INSERT INTO public.playbook_aggregation_state(
        agent_version, pending, next_attempt_at
    ) VALUES (
        v_agent_version, true,
        floor(extract(epoch FROM clock_timestamp()))::bigint
    ) ON CONFLICT (agent_version) DO UPDATE SET pending=true,
        next_attempt_at=LEAST(
            playbook_aggregation_state.next_attempt_at,
            excluded.next_attempt_at
        );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS capture_playbook_aggregation_invalidation
    ON public.lineage_event;
CREATE TRIGGER capture_playbook_aggregation_invalidation
AFTER INSERT ON public.lineage_event
FOR EACH ROW EXECUTE FUNCTION public.capture_playbook_aggregation_invalidation();

CREATE OR REPLACE FUNCTION public.capture_playbook_aggregation_hard_delete()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = ''
AS $$
BEGIN
    IF OLD.status IS NOT NULL OR NULLIF(btrim(OLD.agent_version), '') IS NULL THEN
        RETURN OLD;
    END IF;
    INSERT INTO public.playbook_aggregation_invalidation(
        agent_version, operation, entity_id, source_ids
    ) VALUES (
        OLD.agent_version, 'hard_delete', OLD.user_playbook_id, '[]'::jsonb
    );
    INSERT INTO public.playbook_aggregation_state(
        agent_version, pending, next_attempt_at
    ) VALUES (
        OLD.agent_version, true,
        floor(extract(epoch FROM clock_timestamp()))::bigint
    ) ON CONFLICT (agent_version) DO UPDATE SET pending=true,
        next_attempt_at=LEAST(
            playbook_aggregation_state.next_attempt_at,
            excluded.next_attempt_at
        );
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS capture_playbook_aggregation_hard_delete
    ON public.user_playbooks;
CREATE TRIGGER capture_playbook_aggregation_hard_delete
BEFORE DELETE ON public.user_playbooks
FOR EACH ROW EXECUTE FUNCTION public.capture_playbook_aggregation_hard_delete();

CREATE INDEX IF NOT EXISTS idx_user_playbooks_aggregation_intake
    ON public.user_playbooks(agent_version, user_playbook_id)
    WHERE status IS NULL
      AND NULLIF(btrim(content), '') IS NOT NULL
      AND NULLIF(btrim(agent_version), '') IS NOT NULL;

-- Make the current agent playbook embedding the durable cluster centroid, add
-- the recent-intake floor and cluster repair retry clock, and preserve hourly
-- coalescing when new work arrives.


ALTER TABLE public.playbook_aggregation_state
    ADD COLUMN IF NOT EXISTS intake_floor_user_playbook_id bigint NOT NULL DEFAULT 0;

ALTER TABLE public.playbook_aggregation_cluster
    ADD COLUMN IF NOT EXISTS rebuild_attempt_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rebuild_next_attempt_at bigint NOT NULL DEFAULT 0;

DROP INDEX IF EXISTS public.idx_playbook_aggregation_cluster_rebuild_due;
CREATE INDEX IF NOT EXISTS idx_playbook_aggregation_cluster_rebuild_due
    ON public.playbook_aggregation_cluster(
        agent_version, rebuild_next_attempt_at, cluster_id
    ) WHERE state = 'rebuilding';

DROP INDEX IF EXISTS public.idx_user_playbooks_aggregation_rebuild;
CREATE INDEX IF NOT EXISTS idx_user_playbooks_aggregation_rebuild
    ON public.user_playbooks(agent_version, created_at DESC, user_playbook_id DESC)
    WHERE status IS NULL
      AND NULLIF(btrim(content), '') IS NOT NULL
      AND NULLIF(btrim(agent_version), '') IS NOT NULL;

DROP INDEX IF EXISTS public.idx_playbook_aggregation_item_rebuild;
CREATE INDEX IF NOT EXISTS idx_playbook_aggregation_item_rebuild
    ON public.playbook_aggregation_item(
        agent_version, cluster_id, user_playbook_id DESC
    )
    WHERE disposition = 'residual' AND cluster_id IS NOT NULL;

UPDATE public.playbook_aggregation_cluster AS cluster
SET centroid = agent.embedding,
    vector_sum = NULL
FROM public.agent_playbooks AS agent
WHERE agent.agent_playbook_id = cluster.agent_playbook_id
  AND agent.embedding IS NOT NULL
  AND cluster.vector_sum IS NOT NULL;

CREATE OR REPLACE FUNCTION public.capture_playbook_aggregation_invalidation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_agent_version text;
    v_candidate text;
BEGIN
    IF NEW.entity_type <> 'user_playbook' OR NEW.op NOT IN (
        'create', 'merge', 'revise', 'status_change', 'purge'
    ) THEN
        RETURN NEW;
    END IF;
    IF NEW.entity_id ~ '^[0-9]+$' THEN
        SELECT p.agent_version INTO v_agent_version
        FROM public.user_playbooks p
        WHERE p.user_playbook_id = NEW.entity_id::bigint;
    END IF;
    IF v_agent_version IS NULL THEN
        FOR v_candidate IN SELECT jsonb_array_elements_text(NEW.source_ids) LOOP
            IF v_candidate ~ '^[0-9]+$' THEN
                SELECT p.agent_version INTO v_agent_version
                FROM public.user_playbooks p
                WHERE p.user_playbook_id = v_candidate::bigint;
            END IF;
            EXIT WHEN v_agent_version IS NOT NULL;
        END LOOP;
    END IF;
    IF NULLIF(btrim(v_agent_version), '') IS NULL THEN
        RETURN NEW;
    END IF;
    IF NEW.entity_id !~ '^[0-9]+$' THEN
        RETURN NEW;
    END IF;
    -- Creation only arms intake discovery. It does not invalidate any existing
    -- cluster, and queueing it would let sustained writes starve aggregation
    -- behind an ever-growing list of no-op invalidations.
    IF NEW.op <> 'create' THEN
        INSERT INTO public.playbook_aggregation_invalidation(
            agent_version, operation, entity_id, source_ids
        ) VALUES (
            v_agent_version, NEW.op, NEW.entity_id::bigint, NEW.source_ids
        );
    END IF;
    INSERT INTO public.playbook_aggregation_state(
        agent_version, pending, next_attempt_at
    ) VALUES (
        v_agent_version, true,
        floor(extract(epoch FROM clock_timestamp()))::bigint
    ) ON CONFLICT (agent_version) DO UPDATE SET pending=true;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.capture_playbook_aggregation_hard_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.status IS NOT NULL OR NULLIF(btrim(OLD.agent_version), '') IS NULL THEN
        RETURN OLD;
    END IF;
    INSERT INTO public.playbook_aggregation_invalidation(
        agent_version, operation, entity_id, source_ids
    ) VALUES (
        OLD.agent_version, 'hard_delete', OLD.user_playbook_id, '[]'::jsonb
    );
    INSERT INTO public.playbook_aggregation_state(
        agent_version, pending, next_attempt_at
    ) VALUES (
        OLD.agent_version, true,
        floor(extract(epoch FROM clock_timestamp()))::bigint
    ) ON CONFLICT (agent_version) DO UPDATE SET pending=true;
    RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION public.retire_playbook_aggregation_cluster_for_agent()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_agent_playbook_id bigint;
    v_agent_version text;
    v_reason text;
BEGIN
    IF TG_OP = 'DELETE' THEN
        v_agent_playbook_id := OLD.agent_playbook_id;
        v_reason := 'agent_playbook_deleted';
    ELSE
        IF NOT (
            (NEW.status IS DISTINCT FROM OLD.status AND NEW.status IS NOT NULL)
            OR (
                NEW.playbook_status IS DISTINCT FROM OLD.playbook_status
                AND NEW.playbook_status = 'rejected'
            )
            OR NEW.content IS DISTINCT FROM OLD.content
            OR NEW.trigger IS DISTINCT FROM OLD.trigger
            OR NEW.rationale IS DISTINCT FROM OLD.rationale
            OR NEW.embedding::text IS DISTINCT FROM OLD.embedding::text
        ) THEN
            RETURN NEW;
        END IF;
        v_agent_playbook_id := OLD.agent_playbook_id;
        v_reason := 'agent_playbook_changed';
    END IF;

    FOR v_agent_version IN
        SELECT DISTINCT cluster.agent_version
        FROM public.playbook_aggregation_cluster AS cluster
        WHERE cluster.agent_playbook_id = v_agent_playbook_id
    LOOP
        UPDATE public.playbook_aggregation_item AS item
        SET disposition = 'residual',
            cluster_id = NULL,
            reason = v_reason,
            attempt_count = 0,
            last_attempt_at = NULL,
            updated_at = floor(extract(epoch FROM clock_timestamp()))::bigint
        WHERE item.agent_version = v_agent_version
          AND item.cluster_id IN (
              SELECT cluster.cluster_id
              FROM public.playbook_aggregation_cluster AS cluster
              WHERE cluster.agent_playbook_id = v_agent_playbook_id
          );

        DELETE FROM public.playbook_aggregation_cluster AS cluster
        WHERE cluster.agent_playbook_id = v_agent_playbook_id
          AND cluster.agent_version = v_agent_version;

        INSERT INTO public.playbook_aggregation_state(
            agent_version, pending, next_attempt_at
        ) VALUES (
            v_agent_version, true,
            floor(extract(epoch FROM clock_timestamp()))::bigint
        ) ON CONFLICT (agent_version) DO UPDATE SET pending = true;
    END LOOP;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS retire_playbook_aggregation_cluster_on_agent_update
    ON public.agent_playbooks;
CREATE TRIGGER retire_playbook_aggregation_cluster_on_agent_update
AFTER UPDATE OF status, playbook_status, content, trigger, rationale, embedding
ON public.agent_playbooks
FOR EACH ROW
EXECUTE FUNCTION public.retire_playbook_aggregation_cluster_for_agent();

DROP TRIGGER IF EXISTS retire_playbook_aggregation_cluster_on_agent_delete
    ON public.agent_playbooks;
CREATE TRIGGER retire_playbook_aggregation_cluster_on_agent_delete
BEFORE DELETE ON public.agent_playbooks
FOR EACH ROW
EXECUTE FUNCTION public.retire_playbook_aggregation_cluster_for_agent();
