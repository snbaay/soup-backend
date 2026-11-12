# First prompt for Codex

Read `TASK.md`, `AGENTS.md`, and `design.md`. We are building the Soup Backend Engineer take-home assignment.

First, challenge the design: identify correctness gaps and clarify the precise cancellation/complete race semantics. Update `design.md` to a concise one-page reviewable design BEFORE implementing code. Explain to me in simple Russian the lease, fencing generation, transaction boundaries, and exactly-once committed side effect. Do not code yet.

After we approve the design, implement the smallest runnable FastAPI + Postgres + Docker Compose skeleton with migrations, job API and 4 workers. Make small changes, run actual tests, and report exact commands/results. Leave the chaos harness for a separate phase. Never fabricate logs, test counts, benchmarks, or AI verification.
