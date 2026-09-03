# Runbook

## Cluster stuck in PROVISIONING

1. `kubectl -n dataplane-clusters get pods,sts,svc`
2. `kubectl -n dataplane-clusters describe pod <pod>`
3. `kubectl -n dataplane-clusters logs <pod>`
4. Check laptop memory. OpenSearch requests 768Mi per node and can use up to 1.5Gi.
5. Confirm the image can be pulled.

## API not ready

- `curl http://localhost:8080/healthz`
- `curl http://localhost:8080/readyz`
- Check PostgreSQL with `docker compose ps postgres`.

## Retry behavior

Jobs make at most three attempts. Failed attempts are rescheduled with exponential delays of 1s and 2s; the third failure is terminal. After the final failure the job and cluster are marked `FAILED` and `last_error` is exposed by the cluster API.

## Local security warning

Kubernetes OpenSearch resources set `DISABLE_SECURITY_PLUGIN=true`. Never use this configuration for an internet-accessible or production cluster.
