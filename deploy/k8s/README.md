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

### 2. Configure the namespace and secret

Create the namespace first:

```bash
kubectl apply -f deploy/k8s/00-namespace.yaml
```

Create `reflexio-backend-secrets` with `POSTGRES_DB_URL` and provider keys.
The example secret is intentionally not included in `kustomization.yaml`, so
`kubectl apply -k deploy/k8s` will not apply placeholder secrets.

Use `kubectl create secret`:

```bash
kubectl -n reflexio create secret generic reflexio-backend-secrets \
  --from-literal=POSTGRES_DB_URL='postgresql://reflexio:reflexio@postgres.example.com:5432/reflexio' \
  --from-literal=OPENAI_API_KEY='sk-...' \
  --dry-run=client -o yaml | kubectl apply -f -
```

Or copy and edit the checked-in template:

```bash
cp deploy/k8s/11-secret.example.yaml /tmp/reflexio-secret.yaml
# Edit /tmp/reflexio-secret.yaml with the real POSTGRES_DB_URL and keys, then:
kubectl apply -f /tmp/reflexio-secret.yaml
```

Do not commit a filled secret file.

### 3. Review ConfigMap, storage, and ingress

`deploy/k8s/10-configmap.yaml` sets the non-secret runtime config:

- `REFLEXIO_STORAGE=postgres`
- `REFLEXIO_POSTGRES_SCHEMA=public`
- `REFLEXIO_POSTGRES_POOL_SIZE=5`
- `LOCAL_STORAGE_PATH=/data/reflexio`
- `REFLEXIO_LOG_DIR=/data/reflexio`

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

### 4. Apply and wait for rollout

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

### 5. Smoke test

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
  'printenv REFLEXIO_STORAGE REFLEXIO_POSTGRES_SCHEMA REFLEXIO_POSTGRES_POOL_SIZE; test -n "$POSTGRES_DB_URL" && echo POSTGRES_DB_URL=set'
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

### 6. Update or remove

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
