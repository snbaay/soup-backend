# Codex instructions — Soup Backend Engineer Take-home

You are implementing a failure-aware training-job queue. Do not optimize for the happy path.

## Work rules
- Read `TASK.md` and `design.md` before coding.
- Implement in small reviewable steps; run tests after each step.
- Never claim a chaos test passed unless the command actually completed and the raw output was saved.
- Keep *all* raw test runs, including failures, under `artifacts/runs/`; do not overwrite logs.
- Record each AI suggestion used, accepted, rejected, and manually verified in `AI_USAGE.md`.
- Be explicit about assumptions and failure limitations. Do not pretend exactly-once execution; distinguish execution from exactly-once committed side effect.
- Avoid unnecessary infrastructure: no Redis, Celery, Kubernetes, or message broker unless compellingly justified.
- Use PostgreSQL, Python, FastAPI, SQLAlchemy/psycopg, pytest, Docker Compose. Make `docker compose up` sufficient for the default service.
- After each phase, provide: files changed, commands run, actual outputs, remaining risks, and one short explanation fit for an interview.

## Concurrency and correctness requirements
- Enforce unique idempotency keys in the database, with documented behavior for conflicting payloads.
- Claim jobs atomically with row locks (`FOR UPDATE SKIP LOCKED`) and a lease/fencing generation.
- Use database time for lease comparisons and heartbeat updates. Fence stale worker writes by generation + status.
- Persist the side effect atomically with the final job state where feasible, and give the side-effect table a UNIQUE(job_id) constraint.
- Do not hold a database transaction open while simulating work.
- Define cancellation/complete precedence; cancellation must not allow a later successful commit.
- Reclaim expired running leases while respecting cancellation and stale worker fencing.
- Ensure no jobs remain indefinitely `running` after the harness has stopped inducing crashes and recovery has completed.
- Use explicit, documented transaction isolation; test relevant races under it.

## Test expectations
- Unit/integration tests: idempotent retry, conflicting payload, simultaneous claim, cancelled-before-claim, cancel-vs-complete, expired lease, stale zombie worker, worker SIGKILL.
- Chaos harness: 10,000 jobs, 8 workers, SIGKILL roughly every 2s, concurrently repeated POSTs with same keys and cancellations. Restart workers so progress resumes. Log every event/run with timestamps and seed.
- SQL-based invariant checker: no lost jobs, no duplicate committed side effects, no stuck running jobs, terminal statuses consistent with effects and cancellations.
- Explain limitations of harness (finite scheduling, untested network partitions, external nontransactional effects, host crashes, time bound, etc.).

## Deliverables
Repository, one-page design note, reproducible harness, all raw outputs (success and failure), README with commands, AI_USAGE.md, one paragraph on surprises and remaining concerns, SHIP or DON'T SHIP verdict with evidence at 100 RPS.
