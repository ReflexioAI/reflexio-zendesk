-- Add the missing vector index used by hybrid_match_user_playbooks.
-- Keep extension/opclass references in public; enterprise schema rendering
-- rewrites only the application table target for org-scoped schemas.
CREATE INDEX IF NOT EXISTS "user_playbooks_embedding_idx"
ON "public"."user_playbooks"
USING "ivfflat" ("embedding" "public"."vector_cosine_ops")
WITH ("lists"='100');

ANALYZE "public"."user_playbooks";
