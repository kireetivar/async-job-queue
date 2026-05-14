# Async Job Queue

A distributed async job queue in Go, backed by Redis.

## Architecture

```
Client → Gin API → Redis → Worker Pool → Handler → Business Logic
                     ↑                      ↓
                 Scheduler            Retry Engine
```

- **Redis Store** — HASH for job metadata, ZSET for priority queues, LIST for dead letters
- **Worker Pool** — Fixed goroutine pool with blocking dequeue (`BZPOPMIN`)
- **Retry Engine** — Exponential backoff with jitter, dead letter after max retries
- **Scheduler** — Background ticker that promotes delayed jobs to active queues

## Quick Start

```bash
# Start Redis (WSL)
wsl -e sudo service redis-server start

# Run
go run .
```

## API

```
POST   /api/v1/jobs                 Enqueue a job
GET    /api/v1/jobs/:id             Get job details
DELETE /api/v1/jobs/:id             Cancel a job
POST   /api/v1/jobs/:id/retry       Retry a dead/failed job
GET    /api/v1/queues               List queues
POST   /api/v1/queues/:name/pause   Pause a queue
POST   /api/v1/queues/:name/resume  Resume a queue
GET    /api/v1/dashboard            Queue health stats
```

## Example

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"queue":"email","type":"send_welcome","payload":{"user_id":1},"priority":5,"max_retries":3}'
```
