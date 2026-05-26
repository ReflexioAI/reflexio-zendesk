-- R2 / reflexio-enterprise#59: replace single-slot pending_request_id with a
-- FIFO pending_request_queue so concurrent publishes during a held lock no
-- longer overwrite each other.
--
-- The old function silently dropped earlier blocked requests when a newer one
-- arrived (last-wins). The drain loop also re-ran with the original holder's
-- request payload, so blocked publishes for different users never extracted.
--
-- The queue stores ``{"request_id": text, "payload": jsonb}`` entries in
-- arrival order. Duplicates by ``request_id`` are dropped so publish retries
-- are idempotent. The legacy ``pending_request_id`` field is kept in sync
-- with the most recent enqueued request for a one-deploy back-compat window.

-- Drop the old single-arg function so we can re-create with an extra param.
DROP FUNCTION IF EXISTS "public"."try_acquire_in_progress_lock"(
    "p_state_key" "text",
    "p_request_id" "text",
    "p_stale_lock_seconds" integer
);

CREATE OR REPLACE FUNCTION "public"."try_acquire_in_progress_lock"(
    "p_state_key" "text",
    "p_request_id" "text",
    "p_stale_lock_seconds" integer DEFAULT 300,
    "p_payload" "jsonb" DEFAULT '{}'::jsonb
) RETURNS "jsonb"
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    v_current_state JSONB;
    v_current_time BIGINT;
    v_existing_state JSONB;
    v_existing_queue JSONB;
    v_already_queued BOOLEAN;
    v_holder TEXT;
    v_is_stale BOOLEAN;
    v_in_progress BOOLEAN;
    v_started_at BIGINT;
BEGIN
    v_current_time := EXTRACT(EPOCH FROM NOW())::BIGINT;

    SELECT operation_state INTO v_existing_state
    FROM _operation_state
    WHERE service_name = p_state_key
    FOR UPDATE;

    IF v_existing_state IS NULL THEN
        -- No row — acquire fresh.
        v_current_state := jsonb_build_object(
            'in_progress', true,
            'started_at', v_current_time,
            'current_request_id', p_request_id,
            'pending_request_id', NULL::text,
            'pending_request_queue', '[]'::jsonb
        );
        INSERT INTO _operation_state (service_name, operation_state, updated_at)
        VALUES (p_state_key, v_current_state, NOW());
        RETURN jsonb_build_object('acquired', true, 'state', v_current_state);
    END IF;

    v_in_progress := COALESCE((v_existing_state->>'in_progress')::boolean, false);
    v_started_at := COALESCE((v_existing_state->>'started_at')::bigint, 0);
    v_is_stale := (v_current_time - v_started_at) >= p_stale_lock_seconds;
    v_holder := v_existing_state->>'current_request_id';

    IF NOT v_in_progress OR v_is_stale THEN
        -- No active lock or stale lock — acquire and reset queue.
        v_current_state := jsonb_build_object(
            'in_progress', true,
            'started_at', v_current_time,
            'current_request_id', p_request_id,
            'pending_request_id', NULL::text,
            'pending_request_queue', '[]'::jsonb
        );
        UPDATE _operation_state
        SET operation_state = v_current_state, updated_at = NOW()
        WHERE service_name = p_state_key;
        RETURN jsonb_build_object('acquired', true, 'state', v_current_state);
    END IF;

    -- Holder retry — idempotent acquire.
    IF v_holder IS NOT NULL AND v_holder = p_request_id THEN
        RETURN jsonb_build_object('acquired', true, 'state', v_existing_state);
    END IF;

    -- Active lock held by someone else — append to queue (FIFO, dedup).
    v_existing_queue := COALESCE(v_existing_state->'pending_request_queue', '[]'::jsonb);

    SELECT EXISTS (
        SELECT 1
        FROM jsonb_array_elements(v_existing_queue) AS entry
        WHERE entry->>'request_id' = p_request_id
    ) INTO v_already_queued;

    IF NOT v_already_queued THEN
        v_existing_queue := v_existing_queue
            || jsonb_build_array(
                jsonb_build_object(
                    'request_id', p_request_id,
                    'payload', COALESCE(p_payload, '{}'::jsonb)
                )
            );
    END IF;

    v_current_state := v_existing_state
        || jsonb_build_object(
            'pending_request_queue', v_existing_queue,
            'pending_request_id', p_request_id  -- legacy mirror
        );

    UPDATE _operation_state
    SET operation_state = v_current_state, updated_at = NOW()
    WHERE service_name = p_state_key;

    RETURN jsonb_build_object('acquired', false, 'state', v_current_state);
END;
$$;

ALTER FUNCTION "public"."try_acquire_in_progress_lock"(
    "p_state_key" "text",
    "p_request_id" "text",
    "p_stale_lock_seconds" integer,
    "p_payload" "jsonb"
) OWNER TO "postgres";

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
