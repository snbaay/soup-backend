# Submission checklist (2026-11-13 audit)

- [x] One-page design note written before implementation; updated to match `execution_effects` and current evidence.
- [x] Compose defines PostgreSQL, API and four default workers; startup in this Windows checkout was not rerun.
- [x] Create, status, cursor-paginated list and cancel API implemented.
- [x] Database uniqueness and conflicting-payload behavior implemented and covered by integration tests.
- [x] Row-locked claim, database-time leases, fencing generation, recovery and atomic committed effect implemented.
- [x] Tests cover duplicate keys, double claim, cancellation races, expired leases, zombie writes, clock skew and worker termination.
- [x] User reports 19/19 Codespaces tests and 20 repeat runs of five tests each; source has 17 database-backed and two unit tests in that 19-test set; raw Codespaces logs absent here.
- [x] User reports full 10,000-job/eight-worker chaos run with 181 confirmed SIGKILLs and passing invariant audit; raw run directory absent here.
- [x] SQL/key invariant checker and reproducible harness implemented; missing Codespaces outputs prevent independent final audit.
- [x] Earlier local failures and setup attempts retained under `artifacts/runs/`.
- [x] User reports 3,000- and 10,000-request POST-only runs at 99.98 and 99.97 RPS with zero errors; raw benchmark outputs absent here.
- [x] README documents commands, measured limitations, surprises and evidence-based DON'T SHIP for sustained 100 RPS.
- [x] AI usage and accepted/rejected recommendations documented.
- [ ] Add raw Codespaces integration, repeat, chaos and benchmark logs to `artifacts/runs/` without overwriting previous runs.
- [ ] Verify `docker compose up --build` and sustained representative 100 RPS with workers, retries and cancellations, recording load-time connection/resource data.
- [ ] Share repository and send assignment email; not authorized in this audit.
- [ ] Prepare independent explanation for the live discussion.
