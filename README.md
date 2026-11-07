# Distributed job scheduler

This repository implements a single-job scheduling pipeline: HTTP API, PostgreSQL, NATS JetStream, and workers that call a mock sender (no real email or SMS).

**Iteration 2** adds canceling a single job via `POST /jobs/{id}/cancel`.

**Iteration 3** adds campaigns with template variables and per-recipient cancel via `POST /campaigns/{id}/cancel`. Recovery of stuck `processing` jobs is still out of scope.

## Requirements

- Go 1.25.5 (`GOTOOLCHAIN=local` recommended)
- Docker Compose v2
- Python 3 for demo scripts

Pinned images: PostgreSQL `17.11-alpine`, NATS `2.15.2-alpine`. Libraries: pgx v5.11.0, nats.go v1.53.1, goose v3.27.0.

Local credentials `scheduler` / `scheduler` are for this demo only.

## Run with Docker

```bash
docker compose up --build
```

Published on `127.0.0.1` only. API port defaults to `8080`:

```bash
API_PORT=18080 docker compose up --build
API=http://127.0.0.1:18080 python3 scripts/demo.py
API=http://127.0.0.1:18080 python3 scripts/demo_cancel.py
API=http://127.0.0.1:18080 python3 scripts/demo_campaign.py
```

The `migrate` service applies SQL migrations once. Two worker replicas share one durable NATS consumer.

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
- `GET /jobs/{id}` — full job view including `canceled_at` and `campaign_id` when set

Request body limit: **64 KiB** before JSON decode. Payload object limit: **16 KiB**. `scheduled_at` must be RFC3339 with timezone; stored in UTC with microsecond truncation.

Standalone idempotency uses `jobs.idempotency_key` with `campaign_id IS NULL` (separate namespace from campaign jobs).

Idempotency: same `idempotency_key` and canonical content returns the same job (including `canceled`). Different content → `409 idempotency_conflict`.

### Campaigns

- `POST /campaigns` — create a campaign and one scheduled job per recipient (`201` / `200` replay)
- `GET /campaigns/{id}` — `scheduled_at`, `sender`, `total_jobs`, per-status `counts`
- `POST /campaigns/{id}/cancel` — cancel jobs for selected `recipient_ids` (1–1000 unique IDs)

Campaign request body limit: **1 MiB** before JSON decode. Each template field (`subject`, `body`) up to **8 KiB**. Each rendered job payload still max **16 KiB**. Up to **1000** recipients per campaign when the request fits the body limit.

Templates support variables `[name]` (`[A-Za-z_][A-Za-z0-9_]*`). Substitution runs once at create time in `subject` and `body`; values are plain strings (empty allowed). Escapes: `\\` → `\`, `\[` → `[`, `\]` → `]`. Any other `\` sequence or a bare `]` is a validation error. Literal brackets in JSON must double backslashes, for example:

```json
{"template":{"subject":"Hello \\[name\\]","body":"Plain text"}}
```

Campaign idempotency keys are stored on `campaigns.idempotency_key` (not on `jobs.idempotency_key`). Recipient order does not affect the content hash. Replaying after partial cancel returns the existing campaign without resuming canceled jobs.

Targeted cancel returns `200` with per-recipient `canceled`, `already_canceled`, `too_late`, or `not_found` (request order preserved).

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

## Demo scripts

```bash
python3 scripts/demo.py            # timing and idempotency
python3 scripts/demo_cancel.py     # one job runs, one is canceled (API checks only)
python3 scripts/demo_campaign.py   # campaign, partial cancel, summary + idempotent replay
```

Demos verify statuses and timestamps via GET; they do not count mock sender calls (integration tests do that with `recordingSender`).

## Tests

```bash
gofmt -w .
GOTOOLCHAIN=local go test ./...
GOTOOLCHAIN=local go test -tags=integration -count=1 -timeout 20m ./...
```

Integration tests use paused worker claims, API assertions for cancel, race tests from guaranteed `queued`, and sender call counts.

## Known limitations

- No campaign edit or recurring campaigns
- No automatic retry on sender failure
- No recovery if a worker dies in `processing` after send may have started
- Single outbox publisher assumed
- JSON numbers `1` and `1.0` are different content for idempotency
