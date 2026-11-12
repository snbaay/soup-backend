# Soup training-job queue

Python 3.12, FastAPI, PostgreSQL, SQLAlchemy/psycopg. `docker compose up --build` starts PostgreSQL, schema initialization, the API, and four independent workers. Docker and PostgreSQL were unavailable in the authoring environment; runtime behavior and 100 RPS capacity remain **UNVERIFIED**.

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

**DON'T SHIP at 100 RPS yet.** A reported 100-job Codespaces run found 100 jobs and zero duplicate effects, but recorded zero worker kills; the old harness accepted that result. The fixed harness has not yet been run against PostgreSQL. Run it in Codespaces, inspect all raw failures, then perform the 10,000-job run and measure throughput and latency at a sustained 100 RPS with representative job durations and database capacity. A submission target of 100 POSTs/second is a load input, not proof of sustained production capacity.

Surprises and concerns: the source model already contained the main fencing and cancellation rules, but a heartbeat interval longer than a configured short lease could let a healthy worker lose its claim; the worker now sleeps at most half a lease between heartbeats. The largest remaining uncertainty is actual PostgreSQL behavior under process kills and sustained load, because Docker Desktop could not start due to WSL errors here. No runtime result is claimed from the skipped integration suite.
