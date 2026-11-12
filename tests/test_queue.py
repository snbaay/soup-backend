import threading
import os
import subprocess
import sys
import time
from pathlib import Path

from sqlalchemy import func, select, text, update

from soup import queue
from soup.models import ExecutionEffect, Job


def make_job(factory, key="key", duration_ms=0, should_fail=False):
    with factory() as db:
        job, same = queue.create_job(db, key, duration_ms, should_fail)
        assert same
        return job.id


def test_transactions_use_read_committed(session_factory):
    with session_factory() as db:
        assert db.scalar(text("SHOW transaction_isolation")) == "read committed"


def test_idempotent_retry_and_conflict(session_factory):
    job_id = make_job(session_factory)
    with session_factory() as db:
        same, accepted = queue.create_job(db, "key", 0, False)
        assert accepted and same.id == job_id
    with session_factory() as db:
        conflict, accepted = queue.create_job(db, "key", 1, False)
        assert not accepted and conflict.id == job_id
    with session_factory() as db:
        assert len(list(db.scalars(select(Job)))) == 1


def test_concurrent_duplicate_posts_resolve_to_one_job(session_factory):
    barrier = threading.Barrier(9)
    results = []
    failures = []

    def create(index):
        try:
            barrier.wait()
            with session_factory() as db:
                job, same = queue.create_job(db, "concurrent-key", index % 2, False)
                results.append((job.id, same, index % 2, job.duration_ms))
        except Exception as exc:
            failures.append(exc)

    threads = [threading.Thread(target=create, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=10)
    assert not any(thread.is_alive() for thread in threads)
    assert not failures
    assert len(results) == 8 and len({item[0] for item in results}) == 1
    assert len({persisted_duration for _, _, _, persisted_duration in results}) == 1
    assert sum(same for _, same, _, _ in results) == 4
    assert all(same == (requested_duration == persisted_duration)
               for _, same, requested_duration, persisted_duration in results)


def test_simultaneous_claim_only_one_owner(session_factory):
    job_id = make_job(session_factory)
    barrier = threading.Barrier(3)
    results = []

    def claim(owner):
        barrier.wait()
        with session_factory() as db:
            job = queue.claim_job(db, owner)
            results.append(job.id if job else None)

    threads = [threading.Thread(target=claim, args=(f"worker-{i}",)) for i in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()
    assert results.count(job_id) == 1
    assert results.count(None) == 1


def test_cancel_before_claim(session_factory):
    job_id = make_job(session_factory)
    with session_factory() as db:
        assert queue.cancel_job(db, job_id).status == "cancelled"
    with session_factory() as db:
        assert queue.claim_job(db, "worker") is None
        assert db.get(ExecutionEffect, job_id) is None


def test_cancel_then_complete_and_complete_then_cancel(session_factory):
    first_id = make_job(session_factory, "first")
    with session_factory() as db:
        first = queue.claim_job(db, "worker")
    with session_factory() as db:
        assert queue.cancel_job(db, first_id).status == "cancelled"
    with session_factory() as db:
        assert not queue.complete_job(db, first_id, "worker", first.lease_generation)
    second_id = make_job(session_factory, "second")
    with session_factory() as db:
        second = queue.claim_job(db, "worker")
        assert second.id == second_id
    with session_factory() as db:
        assert queue.complete_job(db, second_id, "worker", second.lease_generation)
    with session_factory() as db:
        assert queue.cancel_job(db, second_id).status == "succeeded"
        assert db.get(ExecutionEffect, second_id) is not None
        assert db.get(ExecutionEffect, first_id) is None


def test_expired_lease_zombie_and_reclaim(session_factory):
    job_id = make_job(session_factory)
    with session_factory() as db:
        first = queue.claim_job(db, "old")
    with session_factory() as db, db.begin():
        db.execute(update(Job).where(Job.id == job_id).values(lease_expires_at=func.clock_timestamp()))
    with session_factory() as db:
        assert not queue.heartbeat(db, job_id, "old", first.lease_generation)
    with session_factory() as db:
        assert not queue.complete_job(db, job_id, "old", first.lease_generation)
    with session_factory() as db:
        second = queue.claim_job(db, "new")
        assert second and second.lease_generation == first.lease_generation + 1
    with session_factory() as db:
        assert not queue.fail_job(db, job_id, "old", first.lease_generation, "late")
    with session_factory() as db:
        assert queue.complete_job(db, job_id, "new", second.lease_generation)
        assert db.get(ExecutionEffect, job_id)


def test_attempt_exhaustion_after_crash(session_factory):
    job_id = make_job(session_factory)
    for attempt in range(3):
        with session_factory() as db:
            claimed = queue.claim_job(db, f"worker-{attempt}")
            assert claimed and claimed.attempts == attempt + 1
        with session_factory() as db, db.begin():
            db.execute(update(Job).where(Job.id == job_id).values(lease_expires_at=func.clock_timestamp()))
    with session_factory() as db:
        assert queue.recover_expired(db) == 1
    with session_factory() as db:
        assert db.get(Job, job_id).status == "failed"
        assert db.get(ExecutionEffect, job_id) is None


def test_work_failure_retries_then_fails(session_factory):
    job_id = make_job(session_factory, should_fail=True)
    for attempt in range(3):
        with session_factory() as db:
            claimed = queue.claim_job(db, f"worker-{attempt}")
        with session_factory() as db:
            assert queue.fail_job(db, job_id, f"worker-{attempt}", claimed.lease_generation, "failed")
    with session_factory() as db:
        assert db.get(Job, job_id).status == "failed"
        assert db.get(ExecutionEffect, job_id) is None


def test_cancel_complete_race_commits_consistent_result(session_factory):
    for index in range(3):
        job_id = make_job(session_factory, f"race-{index}")
        with session_factory() as db:
            claimed = queue.claim_job(db, "racer")
        barrier = threading.Barrier(3)
        outcomes = []

        def cancel():
            barrier.wait()
            with session_factory() as db:
                outcomes.append(queue.cancel_job(db, job_id).status)

        def complete():
            barrier.wait()
            with session_factory() as db:
                outcomes.append(queue.complete_job(db, job_id, "racer", claimed.lease_generation))

        threads = [threading.Thread(target=cancel), threading.Thread(target=complete)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()
        assert len(outcomes) == 2
        with session_factory() as db:
            status = db.get(Job, job_id).status
            effects = list(db.scalars(select(ExecutionEffect).where(ExecutionEffect.job_id == job_id)))
            assert status in ("cancelled", "succeeded")
            assert len(effects) == (1 if status == "succeeded" else 0)


def test_hard_deadline_fences_worker(session_factory):
    job_id = make_job(session_factory)
    with session_factory() as db:
        claimed = queue.claim_job(db, "deadline-worker")
    with session_factory() as db, db.begin():
        db.execute(update(Job).where(Job.id == job_id).values(attempt_deadline_at=func.clock_timestamp()))
    with session_factory() as db:
        assert not queue.heartbeat(db, job_id, "deadline-worker", claimed.lease_generation)
    with session_factory() as db:
        assert not queue.complete_job(db, job_id, "deadline-worker", claimed.lease_generation)


def test_host_clock_skew_does_not_change_lease_decision(session_factory, monkeypatch):
    job_id = make_job(session_factory, "clock-skew")
    with session_factory() as db:
        claimed = queue.claim_job(db, "clock-worker")
    monkeypatch.setattr(time, "time", lambda: -10**12)
    with session_factory() as db:
        assert queue.heartbeat(db, job_id, "clock-worker", claimed.lease_generation)
        assert queue.complete_job(db, job_id, "clock-worker", claimed.lease_generation)


def test_sigkill_then_replacement_recovers_job(session_factory):
    job_id = make_job(session_factory, "sigkill", duration_ms=2000)
    env = dict(os.environ, DATABASE_URL=os.environ["TEST_DATABASE_URL"],
               LEASE_SECONDS="1", ATTEMPT_SECONDS="8", HEARTBEAT_SECONDS="0.2", POLL_SECONDS="0.05")
    root = Path(__file__).resolve().parents[1]
    log_dir = root / "artifacts" / "runs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"sigkill-{time.time_ns()}.log"
    with log_path.open("x", encoding="utf-8") as output:
        first = subprocess.Popen([sys.executable, "-m", "soup.worker"], cwd=root, env=env,
                                 stdout=output, stderr=subprocess.STDOUT)
        second = None
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                with session_factory() as db:
                    status = db.get(Job, job_id).status
                if status == "running":
                    break
                time.sleep(0.05)
            assert status == "running"
            first.kill()  # SIGKILL on POSIX; forced process termination on Windows.
            first.wait(timeout=5)
            second = subprocess.Popen([sys.executable, "-m", "soup.worker"], cwd=root, env=env,
                                      stdout=output, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                with session_factory() as db:
                    job = db.get(Job, job_id)
                    status, attempts = job.status, job.attempts
                if status in ("succeeded", "failed"):
                    break
                time.sleep(0.1)
            assert status == "succeeded" and attempts == 2
            with session_factory() as db:
                assert db.get(ExecutionEffect, job_id) is not None
        finally:
            for process in (first, second):
                if process and process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
