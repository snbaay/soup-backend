# Soup Internship — Backend Engineer Task (faithful checklist)

Deadline 48 hours; anticipated work 4–6h; any stack; service starts with `docker compose up`.

Build a training-job queue service with:
- create job with idempotency key
- get job status
- list jobs with pagination
- cancel job
- states: queued -> running -> succeeded / failed / cancelled
- Postgres or SQLite, and explain choice
- at least 4 workers
- a measurable side effect (row or file write) for number of real runs

Before coding, provide a one-page design: claiming, dead-worker detection, cancellation during running, and whether the guarantee is at-least-once, at-most-once, or exactly-once effect.

Prove using a chaos harness: 10,000 jobs, 8 workers, kill a random worker with `kill -9` about every 2 seconds, concurrently repeated POSTs with same keys and cancellations. Check no lost job, no >1 side effect per job, no stuck running jobs, consistent final states. Include raw output and number of runs. State what the harness misses.

Check silent failures: double claim, zombie after lease expiry writing stale result, cancel/complete race, retry duplicates, transaction isolation, clock skew.

Provide SHIP or DON'T SHIP verdict for 100 requests/second, with first fixes if DON'T SHIP.

Submission: repo, design note, harness and all raw outputs including failed attempts, paragraph about surprises/concerns, emailed to team@trysoup.dev. Declare AI usage, accepted suggestions and independently verified/changed work.

Interview: 40-minute live discussion. Examples: 5-minute frozen worker returns, weakest design point, 10x traffic, why 10,000 is sufficient or insufficient.

Scoring: failure correctness 30%; silent failures 20%; design and code quality 15%; live discussion 35%.
