# Async Job Queue

[![CI](https://github.com/kireetivar/async-job-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/kireetivar/async-job-queue/actions/workflows/ci.yml)

A **Redis-backed asynchronous job queue** in Go: priority scheduling, retries with backoff, delayed jobs, dead-letter queues, pluggable handlers (email simulation + outbound webhooks), REST API, and Prometheus metrics.

Built as a portfolio / learning system that emphasizes production concerns (failure modes, observability, graceful shutdown)—not a drop-in replacement for Sidekiq or SQS.

---

## Features

| Area | What you get |
|------|----------------|
| **Queues** | Named queues, priority via Redis ZSET scores, pause / resume |
| **Workers** | Fixed-size pool, blocking dequeue with `BZPOPMIN` |
| **Retries** | Exponential backoff + jitter; max retries → dead letter (LIST) |
| **Scheduler** | Promotes delayed jobs into active queues on an interval |
| **Handlers** | Registry pattern: `email`, `webhook` (HMAC, timeouts, circuit breaker, metrics) |
| **API** | Enqueue / get / cancel / retry, queue ops, dashboard stats, Swagger |
| **Ops** | Structured logging (`slog`), `/metrics`, `/health` (liveness), `/ready` (Redis readiness) |
| **Ship** | Multi-stage Dockerfile, docker-compose (app + Redis + Prometheus + Grafana), GitHub Actions CI |

---

## Architecture

```text
                    ┌─────────────────────────────────────────┐
                    │                 Gin API                 │
                    │  /api/v1/*  /health  /ready  /metrics   │
                    └───────────────────┬─────────────────────┘
                                        │
                                        ▼
┌──────────────┐   promote delayed    ┌──────────────────────┐
│  Scheduler   │ ──────────────────►  │     Redis store      │
└──────────────┘                      │  HASH  job metadata  │
                                      │  ZSET  priority +    │
                                      │        delayed       │
                                      │  LIST  dead letters  │
                                      └──────────┬───────────┘
                                                 │ BZPOPMIN
                                                 ▼
                                      ┌──────────────────────┐
                                      │    Worker pool       │
                                      │  → Handler registry  │
                                      │  → Retry engine      │
                                      └──────────────────────┘
```

**Why these Redis structures**

- **HASH** — O(1) job document by id (status, payload, retries, timestamps).
- **ZSET** — ordered by score for priority and delayed “run at” scheduling; workers block with `BZPOPMIN`.
- **LIST** — dead-letter sink for exhausted retries / redrive via API.

**Design notes**

- Single Redis instance is the coordination point (multi-instance workers share queues; no leader election for workers).
- Job types are registered at process start; allowed types for the API are derived from the registry.
- Liveness (`/health`) does not touch Redis; readiness (`/ready`) requires a successful Redis `PING` so orchestrators can stop routing traffic without crash-looping on dependency outages.

---

## Quick start

### Option A — Docker Compose (recommended)

```bash
docker compose up --build
```

| Service | URL |
|---------|-----|
| API | http://localhost:9090 |
| Swagger | http://localhost:9090/swagger/index.html |
| Metrics | http://localhost:9090/metrics |
| Prometheus | http://localhost:9091 |
| Grafana | http://localhost:3000 |
| RedisInsight | http://localhost:5540 |

Compose sets `SERVER_PORT=:9090` and `REDIS_ADDR=redis:6379`.

### Option B — Local Go + Redis

```bash
# Redis on localhost:6379 (Docker example)
docker run -d --name redis -p 6379:6379 redis:7-alpine

go run .
# listens on :8080 by default
```

```bash
# or via Makefile
make run
```

### Smoke checks

```bash
# Liveness — process up
curl -s http://localhost:9090/health
# {"status":"ok"}

# Readiness — Redis reachable
curl -s http://localhost:9090/ready
# {"status":"ready"}
```

(Use port **8080** for local `go run`, **9090** for compose.)

---

## Configuration

Environment variables (optional `.env` via `godotenv`):

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_ADDR` | `localhost:6379` | Redis host:port |
| `REDIS_PASSWORD` | _(empty)_ | Redis password |
| `REDIS_DB` | `0` | Redis DB index |
| `SERVER_PORT` | `:8080` | HTTP listen address |
| `WORKER_COUNT` | `10` | Concurrent worker goroutines |
| `QUEUES` | `email,mobile` | Comma-separated queue names |
| `SCHEDULE_INTERVAL_SEC` | `5` | Scheduler tick (seconds) |
| `MAX_PRIORITY` | `10` | Max allowed job priority |
| `MAX_RETRIES` | `10` | Max allowed `max_retries` on create |
| `WEBHOOK_SECRET` | _(empty)_ | HMAC secret for `X-Webhook-Signature` |

---

## API

Base path: **`/api/v1`**. Full OpenAPI UI: `/swagger/index.html`.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/jobs` | Enqueue a job |
| `GET` | `/api/v1/jobs/:id` | Get job by id |
| `DELETE` | `/api/v1/jobs/:id` | Cancel a job |
| `POST` | `/api/v1/jobs/:id/retry` | Retry a dead/failed job |
| `GET` | `/api/v1/queues` | List queues |
| `POST` | `/api/v1/queues/:name/pause` | Pause a queue |
| `POST` | `/api/v1/queues/:name/resume` | Resume a queue |
| `GET` | `/api/v1/dashboard` | Queue health stats |
| `GET` | `/health` | Liveness |
| `GET` | `/ready` | Readiness (Redis PING) |
| `GET` | `/metrics` | Prometheus metrics |

### Create job

Default allowed **queues** come from `QUEUES` (e.g. `email`, `mobile`).  
Default allowed **types**: `email`, `webhook` (from the handler registry).

**Email simulation**

```bash
curl -s -X POST http://localhost:9090/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "email",
    "type": "email",
    "priority": 5,
    "max_retries": 3,
    "payload": {
      "to": "user@example.com",
      "from": "noreply@example.com",
      "subject": "Welcome",
      "body": "Hello from the queue"
    }
  }'
```

**Outbound webhook**

```bash
curl -s -X POST http://localhost:9090/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "email",
    "type": "webhook",
    "priority": 5,
    "max_retries": 3,
    "payload": {
      "url": "https://httpbin.org/post",
      "method": "POST",
      "body": {"event": "job.completed"},
      "timeout_seconds": 10
    }
  }'
```

Webhook behavior highlights:

- Optional **HMAC-SHA256** signature header `X-Webhook-Signature` when `WEBHOOK_SECRET` is set  
- Per-job timeout (default 10s)  
- Non-2xx and network errors feed the **retry engine** and a **per-host circuit breaker**  
- Metrics: `webhook_requests_total`, `webhook_failures_total`, `webhook_duration_seconds`

### Inspect a job

```bash
curl -s http://localhost:9090/api/v1/jobs/<job-id>
```

---

## Observability

### Metrics (Prometheus)

Scraped from `/metrics`. Notable series:

| Metric | Type | Meaning |
|--------|------|---------|
| `jobs_enqueued_total` | counter | Jobs accepted |
| `jobs_processed_total` | counter | Successfully handled |
| `jobs_failed_total` | counter | Handler failures |
| `jobs_retried_total` | counter | Retry re-enqueues |
| `jobs_dead_total` | counter | Moved to DLQ |
| `jobs_processed_duration_seconds` | histogram | Handler latency |
| `active_workers` | gauge | Busy workers |
| `webhook_*` | counter / histogram | Outbound webhook calls |

Compose includes **Prometheus** (`prometheus.yml` scrapes `app:9090`) and **Grafana** on port 3000.

### Logging

Uses Go `log/slog` with job and request context (e.g. job id, URL, status on webhook delivery/failure).

### Health model

| Endpoint | Use for | Checks Redis? |
|----------|---------|----------------|
| `GET /health` | Liveness / process up | No |
| `GET /ready` | Readiness / take traffic | Yes (`PING`) |

---

## Development

**Requirements:** Go **1.25+**, Redis 7+ (or Docker).

```bash
make test      # go test ./...
make build     # fmt, vet, swagger, binary → bin/
make run
make swagger   # regenerate docs/ from annotations
```

**CI:** GitHub Actions (`.github/workflows/ci.yml`) runs `go test ./... -count=1` on push/PR to `main`.

Some store tests use [testcontainers](https://golang.testcontainers.org/) (Docker required).

---

## Project layout

```text
api/          HTTP handlers, validation, routes
config/       Env-based configuration
handlers/     Job type implementations (email, webhook)
metrics/      Prometheus instrumentation
models/       Job and queue types
scheduler/    Delayed-job promotion
store/        Redis store + Store interface
worker/       Pool, registry, retry engine
testutil/     Shared test helpers (Redis container)
docs/         Generated Swagger
```

---

## Limitations

- Single Redis; no multi-region or Redis Cluster story yet  
- No auth / multi-tenancy on the HTTP API  
- Email handler **simulates** send (delay + logs), does not call an SMTP provider  
- Webhook SSRF protections and `Retry-After` parsing are intentionally deferred  
- Not load-tested numbers in this README yet (planned)

---

## License

MIT — see [LICENSE](./LICENSE).
