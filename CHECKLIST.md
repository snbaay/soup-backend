# Submission checklist

- [x] One-page design written before implementation
- [ ] `docker compose up` launches API + Postgres + >=4 workers (configured, runtime unverified)
- [x] Create/status/list(pagination)/cancel API implemented (runtime unverified)
- [x] Enforced idempotency key uniqueness and conflict semantics in schema/source (runtime unverified)
- [x] Side-effect count persisted in DB schema/source (runtime unverified)
- [x] Tests written for silent failures, including clock skew and process kill (integration skipped here)
- [ ] Chaos harness full 10k/8 worker/SIGKILL run (implemented, not executed)
- [x] All performed raw test outputs retained, including failures; future runs must do the same
- [x] SQL invariant checker implemented; runtime unverified
- [x] Explain what harness cannot prove
- [ ] 100 RPS test (not run); evidence-based DON'T SHIP verdict documented
- [x] README explains architecture and reproducible commands
- [x] AI_USAGE.md documents AI use and verification limits
- [x] Paragraph on surprises and remaining concerns
- [ ] Repository shared; email reply prepared
- [ ] Interview practice in own words
