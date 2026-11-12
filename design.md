# Design note — draft (review before implementation)

**Stack:** FastAPI, PostgreSQL, 4 default workers (8 in chaos harness), Docker Compose.

**Data model:** `jobs` (`id`, `idempotency_key` unique, `request_hash`, `status`, `attempts`, `lease_generation`, `lease_expires_at`, `worker_id`, `cancel_requested`, `created_at`, `updated_at`, `finished_at`); `job_effects` (`job_id` unique FK, `effect_payload`, `created_at`). Exact schema to be refined in implementation.

**Claim:** worker atomically selects queued or lease-expired running jobs with `FOR UPDATE SKIP LOCKED`, updates `running`, increments `lease_generation`, assigns worker and database-time lease, commits. Never hold DB lock while doing simulated work.

**Dead worker:** expired leases become eligible for reclaim; generation increments. Using DB clock avoids cross-host wall-clock skew for lease decisions. Heartbeats extend a lease only if the worker still owns the current generation.

**Completion and side effect:** in one transaction, conditionally transition to succeeded only when status=running, matching generation, lease still valid, and cancellation has not won. Insert exactly one job_effects row in the same transaction with unique job_id. Rejected stale completion cannot create an effect. A retry may re-execute simulated work, but only one committed database side effect is allowed.

**Cancellation:** queue cancellations become terminal immediately. Running cancellation sets `cancel_requested=true`; owner checks this cooperatively. A DB transaction decides whether cancellation or completion commits first. The API's exact returned state/semantics should be documented. Expired or killed workers must not bypass cancel. If completion has already committed, cancellation cannot retroactively reverse a committed effect.

**Guarantee:** at-least-once attempts after failure, exactly-once *committed DB effect* with database uniqueness plus atomic transition, not exactly-once actual execution or exactly-once arbitrary external side effects.

**Isolation:** PostgreSQL READ COMMITTED with row locking and conditional updates, verified by concurrent tests. Generation fencing protects against zombie completions.

**Risks:** database is a single dependency; long-running work/heartbeat races; retries under starvation; duplicate external effects if simulation is replaced with a nontransactional API; performance at 100 RPS is unproven without an explicit load test.

**Verdict:** TBD based on real evidence. Do not claim SHIP without running measurable checks.
