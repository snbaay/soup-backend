# Soup training-job queue

Python, FastAPI, PostgreSQL, SQLAlchemy/psycopg. `docker compose up --build` starts PostgreSQL, schema initialization, the API, and four independent workers. The supplied Codespaces chaos results passed database invariants but did not reach 100 primary POST/s; isolated API capacity remains **UNVERIFIED**.

## API

```sh
docker compose up --build
curl -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' -H 'Idempotency-Key: example-1' -d '{"duration_ms":1000,"should_fail":false}'
curl http://localhost:8000/jobs/<job-id>
curl 'http://localhost:8000/jobs?limit=50'
curl -X POST http://localhost:8000/jobs/<job-id>/cancel
```

`POST /jobs` requires a global, retained idempotency key of 1–255 characters. The normalized body has `duration_ms` (default 1000, 0–120000) and `should_fail` (default false). Same key and normalized body return the existing job with 201; a different body returns 409. A lost response can be retried. List pagination uses `(created_at,id)`; a cursor is a position, not a snapshot. Cancellation makes queued/running jobs terminal. If completion commits first, cancellation returns `succeeded`; if cancellation commits first, the effect cannot commit.

## Correctness model

Queue transactions use PostgreSQL `READ COMMITTED`. Claim uses `FOR UPDATE SKIP LOCKED` and increments a lease generation. Worker writes require the same owner, generation, running status, live lease, and live hard deadline. Lease decisions use PostgreSQL `clock_timestamp()`. Completion locks the job, inserts one `execution_effects` row, and sets `succeeded` in one transaction; `job_id` is the effect table's primary key. Work simulation does not hold a transaction. A crash can repeat execution, but at most one database effect commits. External nontransactional effects have no exactly-once guarantee. Expired attempts are reclaimed up to three attempts; an exhausted expired job becomes `failed`. Recovery requires a live worker and database.

## Tests and raw logs

Use only the dedicated `soup_test` database; tests drop and recreate its tables. Every run gets a new raw log in `artifacts/runs/`, including failures.

```powershell
docker compose --profile test up -d test-db
$env:TEST_DATABASE_URL='postgresql+psycopg://soup:soup@localhost:5433/soup_test'
$stamp = Get-Date -Format 'yyyyMMddTHHmmssfff'
.\.venv\Scripts\python.exe -m pytest -q *>&1 | Tee-Object -FilePath "artifacts/runs/pytest-$stamp.log"
```

The integration suite covers same-key retry and conflict under concurrent insertion, simultaneous claim, cancellation before claim, both cancel/complete orders and a race, lease expiry, stale worker heartbeat/completion/failure, deadline, attempt exhaustion, forced worker termination and replacement, database isolation, and host clock skew. Without `TEST_DATABASE_URL`, integration tests skip; a green unit-only run is not database evidence.

## Reproducible chaos run

The host runner creates an API process and eight worker processes against the isolated `soup_chaos` database. It submits 10,000 unique keys at a target 100 POSTs/second and schedules duplicate POSTs and cancellations concurrently. While jobs are running, it sends SIGKILL to a random live worker about every two seconds, verifies the process exited from SIGKILL, and starts a replacement. Killing continues through the recovery poll. A run with killing enabled fails if no worker was killed. SIGKILL mode requires POSIX, such as GitHub Codespaces. Each run has a unique key prefix and unique directory with `events.jsonl`, API/worker stdout, and `audit.json`; failed runs are retained. The final audit is attempted even after a configuration or runtime failure. The verdict separates configuration, runtime, and invariant failures. Run it only after Python dependencies from `requirements.txt` are installed.

```powershell
docker compose --profile chaos up -d chaos-db
$env:CHAOS_DATABASE_URL='postgresql+psycopg://soup:soup@localhost:5434/soup_chaos'
.\.venv\Scripts\python.exe -m scripts.chaos
```

Dry run example:

```powershell
python -m scripts.chaos --jobs 100 --workers 8 --duration-ms 5000 --submit-rps 100 --kill-interval 2 --timeout 240
```

Other controls: `--seed`, `--duplicate-rate`, `--cancel-rate`, `--duration-ms`, `--port`, and `--no-kill`. A completed `run_started` event records the prefix. Recheck independently with:

```powershell
.\.venv\Scripts\python.exe -m scripts.check_invariants --prefix '<prefix-from-log>' --jobs 10000
```

The checker uses SQL joins and `EXISTS` tests, plus a Python comparison of every expected key. It fails on missing keys, duplicates, queued/running jobs after recovery, effects for non-successful jobs, success without an effect, or terminal timestamp mismatches. A passed finite run does not prove all possible schedules, network partitions, host/database crashes, prolonged outages, or external side effects. `create_all` is bootstrap only, not schema migration. Keys have no retention policy. At higher load, polling, the single PostgreSQL store, and connection pool capacity need measurement.

## Production readiness

### Isolated HTTP creation benchmark (Codespaces/Linux)

The chaos runner's `--submit-rps` is a target for scheduling **unique** jobs: it submits index `i` at `submit_start + i / submit_rps` into a 32-thread pool. The same pool also handles duplicate POSTs and cancellations. `primary_post_rps` is `jobs / submit_seconds`, where `submit_seconds` ends only after **all** primary and follow-up requests finish. It therefore includes follow-up drain time and is not a clean measure of the primary HTTP creation path. Each `urllib.request.urlopen` call makes a synchronous request; the client does not configure connection reuse. Each request and event is also printed and flushed to disk. The API is one Uvicorn process with synchronous FastAPI routes; each create performs an `INSERT ... ON CONFLICT DO NOTHING RETURNING`, a row read, and a commit. The SQLAlchemy engine uses `READ COMMITTED` and the default QueuePool (locally: size 5, overflow 10) per process. Eight workers separately poll `recover_expired` and `claim_job` and heartbeat/complete against the same PostgreSQL server. These are plausible bottlenecks, not measured root causes.

Run this separately from chaos and without workers. It uses a new `soup_bench` database in the existing PostgreSQL 16 container, one API process, unique keys, and POST `/jobs` only. It records each HTTP result in a new `artifacts/runs/post-bench-*/requests.jsonl` and prints/writes a summary with successful-request p50/p95/p99 latency, successful and attempted RPS, errors, configured and observed concurrency, connection count, and dispatch lag. Its persistent connection per client thread removes repeated TCP setup as an avoidable client bottleneck. The clock starts at the first scheduled request and stops at the last completed request, so backlog reduces achieved RPS. `duration_ms=0` keeps rows queued when no worker is running; it does not change the API insert path. Do not run this PostgreSQL-dependent benchmark on Windows.

```bash
set -e
mkdir -p artifacts/runs
docker compose --profile chaos up -d chaos-db
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BENCH_DB="soup_bench_$(date -u +%Y%m%d%H%M%S)"
docker compose exec -T chaos-db createdb -U soup "$BENCH_DB" > "artifacts/runs/post-createdb-$STAMP.log" 2>&1
export DATABASE_URL="postgresql+psycopg://soup:soup@127.0.0.1:5434/$BENCH_DB"
python -m uvicorn soup.api:app --host 127.0.0.1 --port 8002 > "artifacts/runs/post-api-$STAMP.log" 2>&1 &
API_PID=$!
trap 'kill "$API_PID" 2>/dev/null || true' EXIT
for i in $(seq 1 100); do curl -fsS http://127.0.0.1:8002/health >/dev/null 2>&1 && break; sleep 0.2; done
curl -fsS http://127.0.0.1:8002/health > "artifacts/runs/post-health-$STAMP.log"
DB_ID="$(docker compose --profile chaos ps -q chaos-db)"
(while kill -0 "$API_PID" 2>/dev/null; do date -Is; ps -p "$API_PID" -o pid,%cpu,%mem,rss,etime; docker stats --no-stream "$DB_ID"; sleep 2; done) > "artifacts/runs/post-resource-$STAMP.log" 2>&1 &
MONITOR_PID=$!
nproc > "artifacts/runs/post-host-$STAMP.log"
free -h >> "artifacts/runs/post-host-$STAMP.log"
python -m scripts.bench_post --url http://127.0.0.1:8002 --requests 3000 --target-rps 100 --concurrency 32 > "artifacts/runs/post-bench-3000-$STAMP.log" 2>&1
python -m scripts.bench_post --url http://127.0.0.1:8002 --requests 10000 --target-rps 100 --concurrency 64 > "artifacts/runs/post-bench-10000-$STAMP.log" 2>&1
kill "$MONITOR_PID"
kill "$API_PID"
wait "$API_PID" || true
```

If startup or a benchmark fails, retain every log and the generated run directory. Record PostgreSQL connection count and I/O before attributing a ceiling to the server. Compare 32 and 64 clients, dispatch lag, error rates, latency tails and connection counts. A 2-core Codespace has limited shared CPU for the API, PostgreSQL and load generator. A 100 RPS target or one short successful run is insufficient for a production verdict; repeat a longer run with representative workers, payloads and resource monitoring. Each invocation creates a fresh database, leaving the original 10,000-job chaos data and outputs untouched.

**DON'T SHIP at 100 RPS yet.** The supplied Codespaces evidence reports a 10,000-job, 8-worker chaos run with 181 real SIGKILLs and 38.69 primary POST/s, and a 2,000-job no-kill baseline with 55.66 primary POST/s. Both passed database invariants; the no-kill run had 1,794 successes, 206 cancellations and no request errors. The metric includes follow-up drain time, so it does not establish the API-only ceiling. Run the isolated benchmark and then repeat sustained load with representative workers. A submission target of 100 POSTs/second is a load input, not proof of sustained production capacity.

Surprises and concerns: the source model already contained the main fencing and cancellation rules, but a heartbeat interval longer than a configured short lease could let a healthy worker lose its claim; the worker now sleeps at most half a lease between heartbeats. The supplied Codespaces chaos run provides finite correctness evidence under process kills. Its rate metric includes client follow-up drain time, leaving API-only throughput, latency tails and sustained 100 RPS capacity unresolved. This Windows workspace still cannot execute PostgreSQL-dependent checks.
