# Distributed job scheduler

This repository implements a single-job scheduling pipeline: HTTP API, PostgreSQL, NATS JetStream, and workers that call a mock sender (no real email or SMS).

**Iteration 2** adds canceling a single job via `POST /jobs/{id}/cancel`.

**Iteration 3** adds campaigns with template variables and per-recipient cancel via `POST /campaigns/{id}/cancel`.

**Iteration 4** adds a web UI at `/ui/` (English), list endpoints, and embeds the production build in the controller Docker image (`-tags embedui`).

**Iteration 5** adds Prometheus metrics, a configurable mock sender (`MOCK_SEND_DELAY`, `MOCK_FAILURE_RATE`), and an optional Compose **`monitoring`** profile (Prometheus + Grafana). Metrics describe **database job state** and **mock sender invocations** — not real message delivery.

**Iteration 6** adds PostgreSQL **processing leases**, controller **recovery** for expired leases, terminal job status **`unknown`** when send may have started but the lease expired, and `MAX_CLAIM_ATTEMPTS` enforced during recovery (not incremented on requeue).

## Requirements

- Go 1.25.5 (`GOTOOLCHAIN=local` recommended)
- Docker Compose v2
- Python 3 for demo scripts
- Node.js 22+ for local UI development (`web/`)

Pinned images: PostgreSQL `17.11-alpine`, NATS `2.15.2-alpine`, Prometheus `v2.55.1`, Grafana `11.4.0`. Libraries: pgx v5.11.0, nats.go v1.53.1, goose v3.27.0, prometheus/client_golang v1.22.0.

Local credentials `scheduler` / `scheduler` are for this demo only.

## Run with Docker

```bash
docker compose up --build
```

Published on `127.0.0.1` only. API port defaults to `8080`. After `docker compose up --build`, open **http://127.0.0.1:8080/ui/** (UI is embedded in the controller image).

```bash
API_PORT=18080 docker compose up --build
API=http://127.0.0.1:18080 python3 scripts/demo.py
API=http://127.0.0.1:18080 python3 scripts/demo_cancel.py
API=http://127.0.0.1:18080 python3 scripts/demo_campaign.py
```

The `migrate` service applies SQL migrations once. Two worker replicas share one durable NATS consumer.

### Upgrading to lease recovery (migration `00006`)

Stop **workers and controller** before applying `00006` on an existing database so in-flight claims are not mixed with the new lease columns. The migration sets an **expired** `lease_until` on existing `processing` rows (`NULL` would never match recovery). After the new binaries are up, the controller recovery loop requeues or marks `unknown` / `failed` per policy.

```bash
docker compose stop worker controller
docker compose run --rm migrate
docker compose up -d controller worker
```

## Run on the host

Processes read **environment variables only** (they do not load `.env` files). Export variables before `go run`:

```bash
docker compose up -d postgres nats
cp .env.example .env
set -a && source .env && set +a
go run ./cmd/controller migrate
go run ./cmd/controller
go run ./cmd/worker
```

## HTTP API

### Create and read jobs

- `POST /jobs` — create a job (`201` + `Location`, or `200` idempotent replay)
- `GET /jobs` — paginated standalone jobs (`campaign_id IS NULL`); query `status`, `limit` (default 20, max 100), `offset`
- `GET /jobs/{id}` — full job view including `canceled_at` and `campaign_id` when set

Request body limit: **64 KiB** before JSON decode. Payload object limit: **16 KiB**. `scheduled_at` must be RFC3339 with timezone; stored in UTC with microsecond truncation.

Standalone idempotency uses `jobs.idempotency_key` with `campaign_id IS NULL` (separate namespace from campaign jobs).

Idempotency: same `idempotency_key` and canonical content returns the same job (including `canceled`). Different content → `409 idempotency_conflict`.

### Campaigns

- `POST /campaigns` — create a campaign and one scheduled job per recipient (`201` / `200` replay)
- `GET /campaigns` — paginated campaign summaries with batched status counts
- `GET /campaigns/{id}` — summary plus `template` and `created_at`
- `GET /campaigns/{id}/jobs` — paginated campaign jobs; sort `recipient_id ASC, id ASC`; query `status`, `limit`, `offset`
- `POST /campaigns/{id}/cancel` — cancel jobs for selected `recipient_ids` (1–1000 unique IDs)
- `POST /campaigns/{id}/cancel-all` — cancel every `scheduled` / `queued` job in the campaign (**empty body**; non-empty → `400 invalid_json`)

Campaign request body limit: **1 MiB** before JSON decode. Each template field (`subject`, `body`) up to **8 KiB**. Each rendered job payload still max **16 KiB**. Up to **1000** recipients per campaign when the request fits the body limit.

Templates support variables `[name]` (`[A-Za-z_][A-Za-z0-9_]*`). Substitution runs once at create time in `subject` and `body`; values are plain strings (empty allowed). Escapes: `\\` → `\`, `\[` → `[`, `\]` → `]`. Any other `\` sequence or a bare `]` is a validation error. Literal brackets in JSON must double backslashes, for example:

```json
{"template":{"subject":"Hello \\[name\\]","body":"Plain text"}}
```

Campaign idempotency keys are stored on `campaigns.idempotency_key` (not on `jobs.idempotency_key`). Recipient order does not affect the content hash. Replaying after partial cancel returns the existing campaign without resuming canceled jobs.

Targeted cancel returns `200` with per-recipient `canceled`, `already_canceled`, `too_late`, or `not_found` (request order preserved).

Cancel-all acquires `FOR UPDATE` on the campaign row first (same as targeted cancel), then bulk-updates pending jobs. Response fields: `canceled_now`, `already_canceled`, `too_late`, `total_jobs` (the three categories sum to `total_jobs`). Replaying cancel-all after everything is pending-canceled returns `canceled_now: 0`. Mass cancel and targeted cancel on the same campaign serialize on the campaign lock; cancel still races worker **claim** on individual `queued` rows (winner decides `canceled` vs `too_late`).

### Cancel a job

- `POST /jobs/{id}/cancel` — **empty body required** (non-empty body → `400 invalid_json`)

| Job status | HTTP | `result` / error |
|------------|------|------------------|
| `scheduled`, `queued` | 200 | `canceled` |
| `canceled` | 200 | `already_canceled` |
| `processing`, `succeeded`, `failed` | 409 | `too_late` |
| missing | 404 | `not_found` |

Success body example:

```json
{"job_id":"…","status":"canceled","result":"canceled"}
```

Cancel is a conditional PostgreSQL update and races with worker claim on `queued` rows; only one side wins. Outbox rows may still be published; workers load status from the database and **Ack without calling the sender** for `canceled` jobs.

### Error shape

```json
{"error":{"code":"validation_error","message":"…","details":[{"field":"…","code":"…"}]}}
```

Codes include: `invalid_json`, `request_too_large`, `validation_error`, `idempotency_conflict`, `not_found`, `too_late`, `internal_error`.

## Processing overview

1. Scheduler moves due `scheduled` jobs to `queued` and inserts outbox rows in one transaction.
2. Outbox loop publishes `job_id` to JetStream outside any DB transaction.
3. Workers claim `queued` → `processing`, record `send_started_at` per `send_attempt_id`, call mock, then finish.
4. While handling a message, workers send JetStream **InProgress** periodically (10s in production) to extend `AckWait`.

Worker skips terminal states (`succeeded`, `failed`, **`canceled`**) with Ack and no send.

## Web UI

- Routes under **`/ui/`** (e.g. `/ui/jobs`, `/ui/campaigns`). Root **`/`** redirects to `/ui/`.
- API stays on `/jobs`, `/campaigns`, `/healthz` — unknown API paths return JSON errors, not the SPA.
- **Production:** UI is embedded when the controller is built with `-tags embedui` (Dockerfile does this).
- **Development:** without embed, `/ui/` returns a short notice; run Vite instead:

```bash
docker compose up -d postgres nats migrate controller worker
cd web && npm install && npm run dev
# open http://127.0.0.1:5173/ui/  (proxies API to :8080)
```

List responses omit full message payloads; open a job to load `GET /jobs/{id}`.

Campaign detail offers **Cancel campaign** (`POST …/cancel-all`) when any job is still `scheduled` or `queued`; the summary panel shows the cancel-all counters, then counts and the job table refresh from the API.

## Demo scripts

```bash
python3 scripts/demo.py            # timing and idempotency
python3 scripts/demo_cancel.py     # one job runs, one is canceled (API checks only)
python3 scripts/demo_campaign.py   # campaign, partial cancel, summary + idempotent replay
```

Demos verify statuses and timestamps via GET; they do not count mock sender calls (integration tests do that with `recordingSender`).

## Metrics and mock sender

### Mock environment (workers)

| Variable | Default | Meaning |
|----------|---------|---------|
| `MOCK_SEND_DELAY` | `0` | Simulated send duration (non-negative). Honors request cancellation. |
| `MOCK_FAILURE_RATE` | `0` | Probability in `[0,1]` of returning `ErrSimulatedSend` (job → `failed` in DB). Invalid values including NaN/Inf stop startup. |

A successful mock `Send` only means the mock accepted the call. Job status `succeeded` in PostgreSQL means the worker recorded that outcome — still not real delivery.

| Variable | Default | Meaning |
|----------|---------|---------|
| `WORKER_LEASE_DURATION` | `45s` | Initial and renewed PostgreSQL lease while processing. |
| `WORKER_LEASE_RENEW_INTERVAL` | `10s` | How often the worker renews the lease (bounded DB retries; loss cancels the attempt). |
| `WORKER_SEND_TIMEOUT` | `90s` | Per-attempt sender deadline (may exceed a single lease period while renewals succeed). |

| Variable (controller) | Default | Meaning |
|----------|---------|---------|
| `RECOVERY_INTERVAL` | `5s` | How often expired leases are recovered. |
| `RECOVERY_BATCH` | `50` | Max jobs per recovery transaction. |
| `MAX_CLAIM_ATTEMPTS` | `5` | After this many successful **claims** (`claim_count`), recovery marks `failed` before send instead of requeueing (requeue does not increment `claim_count`). |

Workers extend the lease during send and while retrying `FinishAttempt`. If the lease cannot be renewed, the attempt stops without writing a stale terminal status; recovery decides later.

On graceful shutdown, in-flight mock sends may still be interrupted: the worker does **not** mark `failed` solely because stop was requested; the JetStream message stays unacked until recovery or a redelivery completes the attempt.

### HTTP endpoints

| Process | Metrics URL |
|---------|-------------|
| Controller | `GET /metrics` on the API port (same as `/jobs`, `/campaigns`, `/ui/`) |
| Worker | `GET /metrics` on `METRICS_ADDR` (default `0.0.0.0:9091` inside the container; not published on the host) |

### Metric names (selection)

**Controller (DB snapshot, refreshed on an interval — not on every scrape):** `scheduler_jobs_by_status{status=…}`, `scheduler_jobs_scheduled_future`, `scheduler_jobs_scheduled_overdue`, `scheduler_outbox_unpublished`, `scheduler_db_snapshot_success`, `scheduler_db_snapshot_last_success_timestamp_seconds`. Until the first successful refresh, job/outbox gauges are omitted (not exported as zero). Failed refresh sets `success=0` and keeps the previous snapshot.

**Workers:** `scheduler_sender_invocations_total{result=success|simulated_error|context_canceled|error}`, `scheduler_sender_call_duration_seconds`, `scheduler_sender_start_lag_seconds` (from `send_started_at - scheduled_at` on the Send path), `scheduler_sender_calls_in_flight`. JetStream redeliveries without a Send call do not increment sender counters.

**Controller (recovery):** `scheduler_recovery_requeued_total`, `scheduler_recovery_unknown_total`, `scheduler_recovery_claim_exhausted_total`.

Go runtime/process metrics are included on both processes.

## Monitoring stack (optional profile)

```bash
export API_PORT=18080
export MOCK_SEND_DELAY=500ms
export MOCK_FAILURE_RATE=0

docker compose --profile monitoring up --build -d --scale worker=1
```

Open **http://127.0.0.1:18080/ui/** when using `API_PORT=18080`, **http://127.0.0.1:9090** (Prometheus), **http://127.0.0.1:3000** (Grafana, `admin` / `admin`). Dashboard **“Scheduler / mock sender (not delivery)”** is provisioned automatically. Worker containers use `stop_grace_period: 30s` so in-flight mock sends (e.g. 12s) can finish before Docker sends SIGKILL after the default 10s stop window.

After creating a large campaign in the UI and once jobs are due, scale workers (keep mock env vars in the shell):

```bash
docker compose --profile monitoring up -d --no-deps --scale worker=4 worker
```

Confirm four **healthy** worker scrape targets:

```bash
curl -s 'http://127.0.0.1:9090/api/v1/targets' | jq '[.data.activeTargets[] | select(.labels.job=="scheduler-workers" and .health=="up")] | length'
```

To demonstrate simulated sender errors after the previous campaign has finished, set `MOCK_FAILURE_RATE=1`, recreate workers so env is applied, run a separate campaign, then restore defaults:

```bash
export MOCK_FAILURE_RATE=1
docker compose --profile monitoring up -d --no-deps --force-recreate worker
# after demo — restore mock defaults
export MOCK_SEND_DELAY=0s
export MOCK_FAILURE_RATE=0
docker compose --profile monitoring up -d --no-deps --force-recreate worker
```

Base `docker compose up` without `--profile monitoring` works unchanged (default **two** worker replicas).

### Graceful stop with an in-flight mock send

Unit tests do not simulate Docker SIGTERM. On a running Compose stack, with one worker and a 12s mock delay:

```bash
export API_PORT=18080
export MOCK_SEND_DELAY=12s
export MOCK_FAILURE_RATE=0
docker compose up -d --no-deps --scale worker=1 worker
API=http://127.0.0.1:18080 python3 scripts/verify_worker_graceful.py
```

The script creates a due job, waits until `send_started_at` is set, runs `docker stop -t 30` on the worker container (`stop_grace_period: 30s` in Compose), and expects the job to reach **`succeeded`**. Restart workers afterward if needed.

### Crash demo (lease recovery, `demorecovery` build only)

The pause-after-`Claim` hook exists only in the `demorecovery` image (`-tags demorecovery`). Use the Compose override and **SIGKILL** (not app context cancel in tests):

```bash
export API_PORT=18080
docker compose -f compose.yaml -f compose.demorecovery.yaml up --build -d --scale worker=1 worker controller
API=http://127.0.0.1:18080 python3 scripts/verify_worker_recovery.py
```

The worker blocks after `Claim` when `RECOVERY_DEMO_PAUSE_AFTER_CLAIM=1`; the script kills the container once the job is `processing` without `send_started_at`, recreates a **standard** worker, then expects **`succeeded`**.

For **`unknown`**, use a normal worker with a long mock send and SIGKILL after `send_started_at`:

```bash
export API_PORT=18080 MOCK_SEND_DELAY=45s
docker compose up -d --scale worker=1 worker controller
API=http://127.0.0.1:18080 python3 scripts/verify_worker_recovery_unknown.py
```

## Local Kubernetes (minikube, `scheduler-local`)

Local cluster scripts never change your default `kubectl` context and **refuse to run** unless `KUBE_CONTEXT=scheduler-local` (your EKS or other contexts are not touched).

**Prerequisites:** Docker Desktop with at least **4 CPUs** and **6 GiB** memory for the profile (application requests for two workers: **500m CPU**, **1152 MiB** memory; limits **2.6 CPU**, **2304 MiB** — excluding Kubernetes system overhead). Profile API server: **Kubernetes v1.31.4**. Scripts use `scripts/k8s/bin/kubectl` (v1.31.4) when present (`scripts/k8s/fetch-kubectl.sh`); your default `kubectl` and `current-context` (e.g. EKS) are unchanged. If port **18080** is already used (e.g. Compose), set `APP_LOCAL_PORT` or use `port-forward.sh` on another port and `API=http://127.0.0.1:<port>` for demos.

```bash
scripts/k8s/cluster-create.sh          # minikube -p scheduler-local --keep-context
scripts/k8s/fetch-kubectl.sh           # optional: client v1.31.4 under scripts/k8s/bin/
scripts/k8s/build-images.sh            # pins tag in scripts/k8s/.image-tag (dirty trees: one timestamp)
scripts/k8s/load-images.sh             # uses the pinned tag
scripts/k8s/deploy-up.sh               # infra → migrate job → app → monitoring
scripts/k8s/port-forward.sh            # UI http://127.0.0.1:18080/ui/
```

`build-images.sh` writes `scripts/k8s/.image-tag` so `load-images.sh` and `deploy-*` use the **same** tag without relying on `export` in the parent shell. Override once for the whole sequence with `IMAGE_TAG=…` before `build-images.sh`, or pass it through each command in the same shell.

**Lifecycle**

| Command | Effect |
|--------|--------|
| `scripts/k8s/stop.sh` | Scale controller, worker, Prometheus, and Grafana to **0**; **keeps** namespace and PVCs |
| `scripts/k8s/destroy-data.sh` | After stop, delete **PVCs** (explicit data wipe) |
| `scripts/k8s/cluster-delete.sh` | `minikube delete -p scheduler-local` — destroys the profile and all data |

**Upgrades:** `scripts/k8s/upgrade-app.sh` rebuilds images (new pin file), loads, migrates (app scaled to zero; waits for controller/worker pods gone), then applies **app only** (`deploy-app.sh`). `deploy-monitoring.sh` applies Prometheus/Grafana only and does not change worker scale or app images.

**Demos on K8s:** run `port-forward.sh`, then `API=http://127.0.0.1:<APP_LOCAL_PORT>` with `scripts/demo*.py`. **`verify_worker_graceful.py` and `verify_worker_recovery*.py` call Docker Compose** (`docker stop` / `docker kill`); they are not Kubernetes checks—use `kubectl` pod delete/SIGKILL helpers under `scripts/k8s/` (e.g. `verify-kill-worker.sh`) and the same HTTP API. Helper checks: `scripts/k8s/verify-prometheus-targets.sh 2`, `scripts/k8s/scale-workers.sh 4`.

Grafana admin credentials are in the demo `scheduler-secrets` Secret (`admin` / `admin`). Worker **startup/readiness** probe HTTP `/metrics` only (no liveness yet). Controller runs **one replica** with `Recreate`.

### Worker autoscaling (KEDA, optional)

Only `Deployment/worker` in namespace `scheduler` is autoscaled. Compose and a minikube stack **without** a ScaledObject keep **manual** two workers (`deploy-up.sh`, `scale-workers.sh`).

Install KEDA chart **`kedacore/keda` version `2.18.3`** (all Helm commands use `--kube-context scheduler-local`):

```bash
scripts/k8s/keda-install.sh
scripts/k8s/autoscale-enable.sh    # ScaledObject worker: min 1, max 4, 50 queued jobs per pod (AverageValue)
scripts/k8s/autoscale-disable.sh   # remove ScaledObject + HPA, workers=2
scripts/k8s/keda-uninstall.sh
```

Prometheus query (same as in `deploy/k8s/autoscaling/scaledobject-worker.yaml`): `scheduler_jobs_by_status{status="queued"}` gated by `scheduler_db_snapshot_success == 1` and snapshot age **&lt; 45s** (`on(job, instance)`). A healthy empty queue returns **one series at 0**; missing/stale snapshot returns **no series** (`ignoreNullValues: false` — no `or vector(0)`). Scale-down stabilization: **60s**.

While a ScaledObject exists: `scale-workers.sh` **errors**; `deploy-app.sh` applies the worker Deployment **without** `spec.replicas` (so reapplies do not reset a hot scale-out). `stop.sh` / `migrate.sh` delete the ScaledObject and wait for the KEDA HPA to disappear, then scale to zero (they do **not** call `autoscale-disable.sh`). After the next `deploy-app.sh`, the stand is manual with **two** workers until `autoscale-enable.sh` runs again.

Verification (not Compose):

```bash
# port-forward controller + prometheus; do not use manual scale during the run
API=http://127.0.0.1:18081 PROM_URL=http://127.0.0.1:9090 python3 scripts/k8s/verify-autoscale.py
```

Writes `verify-autoscale-timeseries.csv` (`timestamp_utc`, `queued`, `desired_replicas`, `ready_replicas`, `worker_targets_up`; empty `queued` when the metric is unavailable). Restores manual mode on exit.

### Worker performance (fixed replicas)

Manual worker scale only (no ScaledObject). The runner saves ConfigMap mock settings, replica count, and autoscale state **before** any change; restores them on exit (including port-forwards it started). Dedicated clean stand recommended: preflight aborts if any job is `scheduled`, `queued`, or `processing` (campaign or standalone).

```bash
scripts/k8s/run-perf-workers.sh --trial   # smoke test (temp dir, removed on success)
API=http://127.0.0.1:18081 PROM_URL=http://127.0.0.1:19090 scripts/k8s/run-perf-workers.sh
```

Full series: nine campaigns × 1000 recipients, `MOCK_SEND_DELAY=500ms`, worker counts `1→2→4`, `2→4→1`, `4→1→2` (three rounds). Artifacts under `perf-workers/run-<UTC>/`: per-run `jobs.csv`, `resources.csv`, `run.json`, plus `metadata.json` and `summary.csv` (medians over three repeats per replica count). Stops on the first failed or incomplete run.

### Cancel impact on campaign timing (2 workers)

Partial cancel before `scheduled_at` via `POST /campaigns/{id}/cancel`. Six scored campaigns (25% / 50% cancel, three rounds: 25→50, 50→25, 25→50); three **0%** controls are **imported** from `perf-workers/run-20261122T160711Z/` (workers=2). Compare **p95 start lag** on succeeded jobs only; Δ in summary = scenario − baseline (negative = shorter).

```bash
PERF_BASELINE_DIR=perf-workers/run-20261122T160711Z \
  API=http://127.0.0.1:18081 PROM_URL=http://127.0.0.1:19090 \
  scripts/k8s/run-cancel-impact.sh --trial
scripts/k8s/run-cancel-impact.sh   # full series
scripts/k8s/rebuild-cancel-summary.py --run-dir perf-cancel-impact/run-<UTC>
```

Artifacts: `perf-cancel-impact/run-<UTC>/` (`cancel_recipients.txt`, `jobs.csv`, `resources.csv`, `runs.jsonl`, `summary.csv`).

## Tests

```bash
gofmt -w .
GOTOOLCHAIN=local go test ./...
GOTOOLCHAIN=local go test -tags=integration -count=1 -timeout 20m ./...
```

Integration tests use paused worker claims, API assertions for cancel, race tests from guaranteed `queued`, and sender call counts.

## Known limitations

- No campaign edit or recurring campaigns
- No automatic retry on sender failure (only lease expiry **requeue** when `send_started_at` is unset)
- After `send_started_at` is set, expiry yields **`unknown`** — no automatic resend; provider callbacks and manual resend are future work
- Graceful shutdown may still leave `processing` until lease recovery; sender timeout with a valid lease yields **`unknown`**
- Single outbox publisher assumed for the hot path (recovery may insert outbox rows concurrently)
- JSON numbers `1` and `1.0` are different content for idempotency
