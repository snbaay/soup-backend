# Submission checklist

- [ ] One-page design written before implementation
- [ ] `docker compose up` launches API + Postgres + >=4 workers
- [ ] Create/status/list(pagination)/cancel API
- [ ] Enforced idempotency key uniqueness and conflict semantics
- [ ] Side-effect count persisted in DB
- [ ] Tests for all six silent failures
- [ ] Chaos harness runs 10k jobs and 8 workers, SIGKILL every ~2s
- [ ] All raw outputs retained, including failed runs
- [ ] Invariant checker reports count and duplicate/missing/stuck statuses
- [ ] Explain what harness cannot prove
- [ ] 100 RPS test and evidence-based SHIP / DON'T SHIP
- [ ] README explains architecture and reproducible commands
- [ ] AI_USAGE.md accurately documents AI use
- [ ] Paragraph on surprises and remaining concerns
- [ ] Repository shared; email reply prepared
- [ ] Interview practice in own words
