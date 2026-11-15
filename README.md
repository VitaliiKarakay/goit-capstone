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

The worker blocks after `Claim` when `RECOVERY_DEMO_PAUSE_AFTER_CLAIM=1`; the script kills the container once the job is `processing` without `send_started_at`, then expects recovery to return the job to **`queued`**.

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
