# Reflexio Kubernetes Backend

These manifests deploy the Reflexio FastAPI backend only. They do not run
Postgres in the backend container or create a Postgres pod. Provide a vanilla
PostgreSQL connection URL through `POSTGRES_DB_URL`. On startup, the backend
applies the prerequisites from `deploy/postgres/001-reflexio-setup.sql` and any
missing app migrations before serving requests. The PVC stores Reflexio's
home/config/log tree at `/data/reflexio/.reflexio`.

## Local container smoke test

```bash
docker build -t reflexio-zendesk:local .
export POSTGRES_DB_URL=postgresql://reflexio:reflexio@host.docker.internal:55432/reflexio
OPENAI_API_KEY=dummy REFLEXIO_EMBEDDING_PROVIDER=inprocess CLAUDE_SMART_USE_LOCAL_EMBEDDING=1 docker compose up -d --no-build reflexio-backend
for i in $(seq 1 60); do curl -fsS http://localhost:8081/health && break || sleep 1; done
ts=$(date +%s)
curl -fsS -A 'Mozilla/5.0 DockerSmoke' -H 'Content-Type: application/json' \
  -X POST http://localhost:8081/api/add_user_profile \
  -d "{\"user_profiles\":[{\"profile_id\":\"docker-smoke-profile\",\"user_id\":\"docker-smoke\",\"content\":\"Docker Postgres smoke profile ${ts}\",\"last_modified_timestamp\":${ts},\"generated_from_request_id\":\"docker-smoke-request\",\"custom_features\":{\"source\":\"docker-e2e\"},\"source\":\"docker-e2e\"}]}"
curl -fsS -A 'Mozilla/5.0 DockerSmoke' -H 'Content-Type: application/json' \
  -X POST http://localhost:8081/api/get_profiles \
  -d '{"user_id":"docker-smoke"}'
psql postgresql://reflexio:reflexio@localhost:55432/reflexio -c '\dx vector'
psql postgresql://reflexio:reflexio@localhost:55432/reflexio -c '\dt public.*'
```

At least one generation-capable provider key must be set in your shell or
Compose `.env` file for startup validation. The smoke command above uses a
dummy OpenAI key plus in-process local embeddings, so the add/read profile path
does not call a cloud embedding API.

For local testing without an existing Postgres instance, start a separate
database container first:

```bash
docker run -d --name reflexio-postgres \
  -e POSTGRES_DB=reflexio \
  -e POSTGRES_USER=reflexio \
  -e POSTGRES_PASSWORD=reflexio \
  -p 55432:5432 \
  -v "$PWD/deploy/postgres/001-reflexio-setup.sql:/docker-entrypoint-initdb.d/001-reflexio-setup.sql:ro" \
  pgvector/pgvector:pg16
until docker exec reflexio-postgres pg_isready -U reflexio -d reflexio; do sleep 1; done
```

Reflexio app migrations run from the backend on first Postgres storage
creation. The setup SQL only prepares database prerequisites.

For local OpenSearch-backed search verification without an AWS OpenSearch
account, start the test-only OpenSearch profile and point the backend at the
Compose service name:

```bash
docker compose --profile opensearch up -d opensearch
export POSTGRES_DB_URL=postgresql://reflexio:reflexio@host.docker.internal:55433/reflexio
OPENAI_API_KEY=dummy \
  REFLEXIO_EMBEDDING_PROVIDER=inprocess \
  CLAUDE_SMART_USE_LOCAL_EMBEDDING=1 \
  REFLEXIO_POSTGRES_SEARCH_BACKEND=opensearch \
  REFLEXIO_OPENSEARCH_ENDPOINT=http://opensearch:9200 \
  REFLEXIO_OPENSEARCH_AUTH=none \
  docker compose --profile opensearch up -d --no-build reflexio-backend
curl -fsS http://localhost:19200/_cluster/health
```

`REFLEXIO_OPENSEARCH_AUTH=none` is only for this local Docker path. AWS
deployments should leave the default `aws_sigv4` auth mode and provide
credentials through IAM role or the standard AWS environment variables.

## Kubernetes deployment

### Prerequisites

- A Kubernetes cluster with `kubectl` configured.
- A reachable PostgreSQL database with pgvector available. The manifests do
  not create a Postgres pod. For local clusters, a host-exposed Postgres URL
  such as `postgresql://reflexio:reflexio@host.docker.internal:55432/reflexio`
  works with Docker Desktop. For cloud clusters, use the provider's private
  service DNS name or managed database endpoint.
- At least one real LLM provider key for generation requests. For a local
  smoke-only deployment, you can use `OPENAI_API_KEY=dummy` together with
  `REFLEXIO_EMBEDDING_PROVIDER=inprocess` and
  `CLAUDE_SMART_USE_LOCAL_EMBEDDING=1`.
- An ingress controller and TLS secret if you plan to expose the service
  through `50-ingress.yaml`. You can skip ingress and use port-forwarding for
  local validation.

### 1. Build and load the image

Build the image from the repository root:

```bash
docker build -t reflexio-zendesk:local .
```

Make the image available to your cluster. For local clusters, load it directly:

```bash
kind load docker-image reflexio-zendesk:local
# or: minikube image load reflexio-zendesk:local
```

For a remote cluster, push the image to a registry and update
`deploy/k8s/30-deployment.yaml`:

```bash
docker tag reflexio-zendesk:local registry.example.com/reflexio-zendesk:latest
docker push registry.example.com/reflexio-zendesk:latest
# then set image: registry.example.com/reflexio-zendesk:latest
```

### 2. Configure the namespace

Create the namespace first:

```bash
kubectl apply -f deploy/k8s/00-namespace.yaml
```

### 3. Choose the Postgres search backend

`REFLEXIO_STORAGE=postgres` controls the primary data store. Query search for
that Postgres store is controlled separately by
`REFLEXIO_POSTGRES_SEARCH_BACKEND`:

| Value | Search engine | Required setup |
| --- | --- | --- |
| `postgres` | Built-in Postgres RPC/pgvector search | Postgres with pgvector only |
| `opensearch` | AWS OpenSearch sidecar search | Postgres plus OpenSearch endpoint/credentials |

Use `postgres` when you want the simplest deployment or do not have OpenSearch
yet. Profiles, interactions, user playbooks, and agent playbooks are searched
through the Postgres functions created by the app migrations.

```yaml
data:
  REFLEXIO_STORAGE: postgres
  REFLEXIO_POSTGRES_SEARCH_BACKEND: postgres
```

Use `opensearch` when you want Postgres as the source of truth but OpenSearch
to execute query search. Startup creates OpenSearch indexes and, when
`REFLEXIO_OPENSEARCH_SYNC_ON_STARTUP=true`, syncs existing Postgres rows before
serving search. Write/update/delete paths then keep OpenSearch in sync.

```yaml
data:
  REFLEXIO_STORAGE: postgres
  REFLEXIO_POSTGRES_SEARCH_BACKEND: opensearch
  REFLEXIO_OPENSEARCH_AUTH: aws_sigv4
  REFLEXIO_OPENSEARCH_REGION: us-west-2
  REFLEXIO_OPENSEARCH_SERVICE: es
  REFLEXIO_OPENSEARCH_INDEX_PREFIX: reflexio
  REFLEXIO_OPENSEARCH_SYNC_ON_STARTUP: "true"
```

For AWS OpenSearch Service, keep `REFLEXIO_OPENSEARCH_AUTH=aws_sigv4` and
grant the pod credentials permission to access the domain, either through your
cluster's workload identity mechanism or standard AWS environment variables.
Use `REFLEXIO_OPENSEARCH_SERVICE=aoss` only for OpenSearch Serverless.

If `REFLEXIO_POSTGRES_SEARCH_BACKEND=opensearch` is set but
`REFLEXIO_OPENSEARCH_ENDPOINT` is missing, backend startup fails loudly. If
`REFLEXIO_POSTGRES_SEARCH_BACKEND=postgres`, OpenSearch config is ignored.

#### OpenSearch setup steps

1. Create or choose an AWS OpenSearch domain that the backend pod can reach.
   Use the domain HTTPS endpoint without a trailing slash, for example
   `https://search-domain.us-west-2.es.amazonaws.com`.
2. Grant the backend pod credentials access to the OpenSearch domain. Prefer
   your cluster's AWS workload identity mechanism, such as IRSA on EKS. If you
   are not using workload identity, provide standard AWS credentials through
   your secret management flow instead.
3. In `deploy/k8s/10-configmap.yaml`, keep Postgres as the source of truth and
   select OpenSearch for query search:

```yaml
data:
  REFLEXIO_STORAGE: postgres
  REFLEXIO_POSTGRES_SEARCH_BACKEND: opensearch
  REFLEXIO_OPENSEARCH_AUTH: aws_sigv4
  REFLEXIO_OPENSEARCH_REGION: us-west-2
  REFLEXIO_OPENSEARCH_SERVICE: es
  REFLEXIO_OPENSEARCH_INDEX_PREFIX: reflexio
  REFLEXIO_OPENSEARCH_SYNC_ON_STARTUP: "true"
```

4. If you use OpenSearch Serverless instead of a managed OpenSearch Service
   domain, set:

```yaml
data:
  REFLEXIO_OPENSEARCH_SERVICE: aoss
```

5. Put the OpenSearch endpoint in the Kubernetes secret together with
   `POSTGRES_DB_URL` and provider keys:

```bash
kubectl -n reflexio create secret generic reflexio-backend-secrets \
  --from-literal=POSTGRES_DB_URL='postgresql://reflexio:reflexio@postgres.example.com:5432/reflexio' \
  --from-literal=REFLEXIO_OPENSEARCH_ENDPOINT='https://search-domain.us-west-2.es.amazonaws.com' \
  --from-literal=OPENAI_API_KEY='sk-...' \
  --dry-run=client -o yaml | kubectl apply -f -
```

6. Apply the manifests and wait for rollout:

```bash
kubectl apply -k deploy/k8s
kubectl -n reflexio rollout status deploy/reflexio-backend
```

7. Confirm the pod has the expected non-secret OpenSearch settings:

```bash
kubectl -n reflexio exec deploy/reflexio-backend -- sh -lc \
  'printenv REFLEXIO_POSTGRES_SEARCH_BACKEND REFLEXIO_OPENSEARCH_AUTH REFLEXIO_OPENSEARCH_REGION REFLEXIO_OPENSEARCH_SERVICE REFLEXIO_OPENSEARCH_INDEX_PREFIX REFLEXIO_OPENSEARCH_SYNC_ON_STARTUP; test -n "$REFLEXIO_OPENSEARCH_ENDPOINT" && echo REFLEXIO_OPENSEARCH_ENDPOINT=set'
```

8. Check the backend logs for OpenSearch startup or credential errors:

```bash
kubectl -n reflexio logs deploy/reflexio-backend --tail=200
```

The application creates and updates its own OpenSearch indexes. You do not
need to pre-create indexes for a new deployment.

### 4. Configure secrets

Create `reflexio-backend-secrets` with `POSTGRES_DB_URL` and provider keys.
Add `REFLEXIO_OPENSEARCH_ENDPOINT` only when using
`REFLEXIO_POSTGRES_SEARCH_BACKEND=opensearch`. The example secret is
intentionally not included in `kustomization.yaml`, so `kubectl apply -k
deploy/k8s` will not apply placeholder secrets.

Use `kubectl create secret`:

Postgres search:

```bash
kubectl -n reflexio create secret generic reflexio-backend-secrets \
  --from-literal=POSTGRES_DB_URL='postgresql://reflexio:reflexio@postgres.example.com:5432/reflexio' \
  --from-literal=OPENAI_API_KEY='sk-...' \
  --dry-run=client -o yaml | kubectl apply -f -
```

OpenSearch search:

```bash
kubectl -n reflexio create secret generic reflexio-backend-secrets \
  --from-literal=POSTGRES_DB_URL='postgresql://reflexio:reflexio@postgres.example.com:5432/reflexio' \
  --from-literal=REFLEXIO_OPENSEARCH_ENDPOINT='https://search-domain.us-west-2.es.amazonaws.com' \
  --from-literal=OPENAI_API_KEY='sk-...' \
  --dry-run=client -o yaml | kubectl apply -f -
```

Or copy and edit the checked-in template:

```bash
cp deploy/k8s/11-secret.example.yaml /tmp/reflexio-secret.yaml
# Edit /tmp/reflexio-secret.yaml with the real POSTGRES_DB_URL, provider keys,
# and REFLEXIO_OPENSEARCH_ENDPOINT only if using OpenSearch search. Then:
kubectl apply -f /tmp/reflexio-secret.yaml
```

Do not commit a filled secret file.

### 5. Review ConfigMap, storage, and ingress

`deploy/k8s/10-configmap.yaml` sets the non-secret runtime config:

- `REFLEXIO_STORAGE=postgres`
- `REFLEXIO_POSTGRES_SCHEMA=public`
- `REFLEXIO_POSTGRES_POOL_SIZE=5`
- `REFLEXIO_POSTGRES_SEARCH_BACKEND=opensearch`
- `REFLEXIO_OPENSEARCH_AUTH=aws_sigv4`
- `REFLEXIO_OPENSEARCH_REGION=us-west-2`
- `REFLEXIO_OPENSEARCH_SERVICE=es`
- `REFLEXIO_OPENSEARCH_INDEX_PREFIX=reflexio`
- `LOCAL_STORAGE_PATH=/data/reflexio`
- `REFLEXIO_LOG_DIR=/data/reflexio`

For Postgres search, set:

```yaml
  REFLEXIO_POSTGRES_SEARCH_BACKEND: postgres
```

The OpenSearch entries may remain in the ConfigMap, but they are ignored while
the search backend is `postgres`.

For OpenSearch search, set:

```yaml
  REFLEXIO_POSTGRES_SEARCH_BACKEND: opensearch
  REFLEXIO_OPENSEARCH_AUTH: aws_sigv4
  REFLEXIO_OPENSEARCH_REGION: us-west-2
  REFLEXIO_OPENSEARCH_SERVICE: es
  REFLEXIO_OPENSEARCH_INDEX_PREFIX: reflexio
  REFLEXIO_OPENSEARCH_SYNC_ON_STARTUP: "true"
```

Also put the endpoint in `reflexio-backend-secrets` as
`REFLEXIO_OPENSEARCH_ENDPOINT`.

If you want in-process local embeddings for a development cluster, add these to
the ConfigMap before applying:

```yaml
  REFLEXIO_EMBEDDING_PROVIDER: inprocess
  CLAUDE_SMART_USE_LOCAL_EMBEDDING: "1"
```

`deploy/k8s/20-pvc.yaml` creates a 5Gi `ReadWriteOnce` PVC for Reflexio's local
home/config/log directory. Adjust the storage size or storage class for your
cluster if needed.

`deploy/k8s/50-ingress.yaml` is a template. Update the host,
`ingressClassName`, and `tls.secretName`, or remove `50-ingress.yaml` from
`deploy/k8s/kustomization.yaml` and use port-forwarding only.

### 6. Apply and wait for rollout

Render the manifests first to catch YAML/Kustomize mistakes:

```bash
kubectl kustomize deploy/k8s
```

Apply the workload:

```bash
kubectl apply -k deploy/k8s
kubectl -n reflexio rollout status deploy/reflexio-backend
```

Inspect pod state if rollout does not complete:

```bash
kubectl -n reflexio get pods
kubectl -n reflexio describe pod -l app.kubernetes.io/name=reflexio-backend
kubectl -n reflexio logs deploy/reflexio-backend --tail=200
```

### 7. Smoke test

For a local check without ingress, port-forward the service:

```bash
kubectl -n reflexio port-forward svc/reflexio-backend 8081:8081
```

In another shell, verify health and a storage-backed write/read flow:

```bash
curl -fsS http://localhost:8081/health
ts=$(date +%s)
curl -fsS -A 'Mozilla/5.0 K8sSmoke' -H 'Content-Type: application/json' \
  -X POST http://localhost:8081/api/add_user_profile \
  -d "{\"user_profiles\":[{\"profile_id\":\"k8s-smoke-profile\",\"user_id\":\"k8s-smoke\",\"content\":\"K8s Postgres smoke profile ${ts}\",\"last_modified_timestamp\":${ts},\"generated_from_request_id\":\"k8s-smoke-request\",\"custom_features\":{\"source\":\"k8s-e2e\"},\"source\":\"k8s-e2e\"}]}"
curl -fsS -A 'Mozilla/5.0 K8sSmoke' -H 'Content-Type: application/json' \
  -X POST http://localhost:8081/api/get_profiles \
  -d '{"user_id":"k8s-smoke"}'
```

Confirm the backend is using Postgres:

```bash
kubectl -n reflexio exec deploy/reflexio-backend -- sh -lc \
  'printenv REFLEXIO_STORAGE REFLEXIO_POSTGRES_SCHEMA REFLEXIO_POSTGRES_POOL_SIZE REFLEXIO_POSTGRES_SEARCH_BACKEND; test -n "$POSTGRES_DB_URL" && echo POSTGRES_DB_URL=set'
```

Then inspect your Postgres database from a machine that can reach it:

```bash
psql "$POSTGRES_DB_URL" -c '\dx vector'
psql "$POSTGRES_DB_URL" -c '\dt public.*'
psql "$POSTGRES_DB_URL" -c 'select version, name, applied_at from supabase_migrations.schema_migrations order by version;'
```

The first backend startup against a fresh database may take longer because it
creates extensions, creates Reflexio tables/functions, and records app
migrations. Later restarts skip already-applied migrations.

### 8. Update or remove

After changing manifests or a pushed image tag:

```bash
kubectl apply -k deploy/k8s
kubectl -n reflexio rollout restart deploy/reflexio-backend
kubectl -n reflexio rollout status deploy/reflexio-backend
```

To remove the Kubernetes resources:

```bash
kubectl delete -k deploy/k8s
kubectl delete secret -n reflexio reflexio-backend-secrets
```

Deleting the namespace or PVC can remove local config/log data. The external
Postgres database is not deleted by these manifests.

## MongoDB

MongoDB is not wired into the app yet. Adding it later requires a Reflexio
storage adapter, config schema, dependency, and `REFLEXIO_STORAGE=mongo`
bootstrap wiring before Kubernetes can point the backend at managed MongoDB.
