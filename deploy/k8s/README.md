# Reflexio Kubernetes Backend

These manifests deploy the Reflexio FastAPI backend only. They intentionally use
one replica with SQLite on a PVC. Do not scale replicas above one until Reflexio
is configured for a networked storage backend. The PVC stores both
`LOCAL_STORAGE_PATH=/data/reflexio` and Reflexio's home/config/log tree at
`/data/reflexio/.reflexio`.

## Local container smoke test

```bash
docker build -t reflexio-zendesk:local .
docker compose up
curl -fsS http://localhost:8081/health
```

At least one LLM provider key must be set in your shell or Compose `.env` file,
for example `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`.

## Kubernetes deployment

1. Build the local image:

   ```bash
   docker build -t reflexio-zendesk:local .
   ```

2. Make the image available to the cluster. For example:

   ```bash
   kind load docker-image reflexio-zendesk:local
   # or: minikube image load reflexio-zendesk:local
   ```

3. Create `reflexio-backend-secrets` with at least one real LLM provider key.
   Use your secret-management tool, or start from the checked-in example:

   ```bash
   kubectl apply -f deploy/k8s/00-namespace.yaml
   cp deploy/k8s/11-secret.example.yaml /tmp/reflexio-secret.yaml
   # edit /tmp/reflexio-secret.yaml with real keys, then:
   kubectl apply -f /tmp/reflexio-secret.yaml
   ```

4. Update `50-ingress.yaml` with your real host, ingress class, and TLS secret.

5. Apply:

   ```bash
   kubectl apply -k deploy/k8s
   kubectl -n reflexio rollout status deploy/reflexio-backend
   kubectl -n reflexio port-forward svc/reflexio-backend 8081:8081
   curl -fsS http://localhost:8081/health
   ```

## MongoDB

MongoDB is not wired into the app yet. Adding it later requires a Reflexio
storage adapter, config schema, dependency, and `REFLEXIO_STORAGE=mongo`
bootstrap wiring before Kubernetes can point the backend at managed MongoDB.
