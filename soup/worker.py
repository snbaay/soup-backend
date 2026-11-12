import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

from soup.db import SessionLocal, init_db, settings
from soup import queue


logging.basicConfig(level=logging.INFO, format="%(message)s")
worker_id = f"{os.getenv('HOSTNAME', 'worker')}-{os.getpid()}-{uuid.uuid4()}"


def log(event: str, **fields):
    logging.info(json.dumps({"timestamp": datetime.now(timezone.utc).isoformat(), "event": event,
                             "worker_id": worker_id, **fields}, default=str))


def run_one(job) -> None:
    # Wall-clock time only controls simulation pacing; database time controls ownership.
    remaining = job.duration_ms / 1000
    while remaining > 0:
        # A long configured heartbeat interval must not outlive a short lease.
        step = min(settings.heartbeat_seconds, settings.lease_seconds / 2, remaining)
        time.sleep(step)
        remaining -= step
        with SessionLocal() as db:
            if not queue.heartbeat(db, job.id, worker_id, job.lease_generation):
                log("ownership_lost", job_id=job.id, generation=job.lease_generation)
                return
    with SessionLocal() as db:
        if job.should_fail:
            ok = queue.fail_job(db, job.id, worker_id, job.lease_generation, "simulated failure")
            event = "attempt_failed"
        else:
            ok = queue.complete_job(db, job.id, worker_id, job.lease_generation)
            event = "completed"
    log(event if ok else "ownership_lost", job_id=job.id, generation=job.lease_generation)


def main() -> None:
    init_db()
    log("started")
    while True:
        try:
            with SessionLocal() as db:
                recovered = queue.recover_expired(db)
            if recovered:
                log("recovered_exhausted", count=recovered)
            with SessionLocal() as db:
                job = queue.claim_job(db, worker_id)
            if job:
                log("claimed", job_id=job.id, generation=job.lease_generation, attempt=job.attempts)
                run_one(job)
            else:
                time.sleep(settings.poll_seconds)
        except Exception as exc:
            log("worker_error", error=repr(exc))
            time.sleep(1)


if __name__ == "__main__":
    main()
