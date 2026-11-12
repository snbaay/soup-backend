import hashlib
import json
import uuid
from datetime import datetime, timedelta

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from soup.db import settings
from soup.models import ExecutionEffect, Job


def body_hash(duration_ms: int, should_fail: bool) -> str:
    canonical = json.dumps({"duration_ms": duration_ms, "should_fail": should_fail}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def create_job(db: Session, key: str, duration_ms: int, should_fail: bool) -> tuple[Job, bool]:
    digest = body_hash(duration_ms, should_fail)
    with db.begin():
        statement = insert(Job).values(id=uuid.uuid4(), idempotency_key=key, request_hash=digest,
                                       duration_ms=duration_ms, should_fail=should_fail)
        created_id = db.execute(statement.on_conflict_do_nothing(index_elements=[Job.idempotency_key]).returning(Job.id)).scalar_one_or_none()
        job = db.get(Job, created_id) if created_id else db.execute(select(Job).where(Job.idempotency_key == key)).scalar_one()
        return job, job.request_hash == digest


def get_job(db: Session, job_id: uuid.UUID) -> Job | None:
    return db.get(Job, job_id)


def list_jobs(db: Session, limit: int, cursor: tuple[datetime, uuid.UUID] | None) -> list[Job]:
    query = select(Job).order_by(Job.created_at, Job.id).limit(limit + 1)
    if cursor:
        created_at, job_id = cursor
        query = query.where(or_(Job.created_at > created_at, and_(Job.created_at == created_at, Job.id > job_id)))
    return list(db.scalars(query))


def cancel_job(db: Session, job_id: uuid.UUID) -> Job | None:
    with db.begin():
        job = db.execute(select(Job).where(Job.id == job_id).with_for_update()).scalar_one_or_none()
        if job and job.status in ("queued", "running"):
            now = db.scalar(select(func.clock_timestamp()))
            job.status = "cancelled"
            job.worker_id = None
            job.lease_expires_at = None
            job.attempt_deadline_at = None
            job.finished_at = now
            job.updated_at = now
        return job


def claim_job(db: Session, worker_id: str) -> Job | None:
    with db.begin():
        eligible = or_(Job.status == "queued", and_(Job.status == "running", Job.lease_expires_at <= func.clock_timestamp()))
        job = db.execute(select(Job).where(eligible, Job.attempts < settings.max_attempts)
                         .order_by(Job.created_at, Job.id).limit(1).with_for_update(skip_locked=True)).scalar_one_or_none()
        if job is None:
            return None
        now = db.scalar(select(func.clock_timestamp()))
        if job.status == "running" and (job.lease_expires_at is None or job.lease_expires_at > now):
            return None
        if job.status not in ("queued", "running") or job.attempts >= settings.max_attempts:
            return None
        lease = now + timedelta(seconds=settings.lease_seconds)
        deadline = now + timedelta(seconds=settings.attempt_seconds)
        job.status = "running"
        job.attempts += 1
        job.lease_generation += 1
        job.worker_id = worker_id
        job.lease_expires_at = min(lease, deadline)
        job.attempt_deadline_at = deadline
        job.updated_at = now
        return job


def heartbeat(db: Session, job_id: uuid.UUID, worker_id: str, generation: int) -> bool:
    with db.begin():
        now = func.clock_timestamp()
        new_expiry = func.least(now + func.make_interval(0, 0, 0, 0, 0, 0, settings.lease_seconds), Job.attempt_deadline_at)
        result = db.execute(update(Job).where(Job.id == job_id, Job.status == "running", Job.worker_id == worker_id,
                                              Job.lease_generation == generation, Job.lease_expires_at > now,
                                              Job.attempt_deadline_at > now)
                            .values(lease_expires_at=new_expiry, updated_at=now))
        return result.rowcount == 1


def _owned(db: Session, job_id: uuid.UUID, worker_id: str, generation: int) -> tuple[Job | None, datetime]:
    job = db.execute(select(Job).where(Job.id == job_id).with_for_update()).scalar_one_or_none()
    now = db.scalar(select(func.clock_timestamp()))
    if not job or job.status != "running" or job.worker_id != worker_id or job.lease_generation != generation:
        return None, now
    if not job.lease_expires_at or not job.attempt_deadline_at or job.lease_expires_at <= now or job.attempt_deadline_at <= now:
        return None, now
    return job, now


def complete_job(db: Session, job_id: uuid.UUID, worker_id: str, generation: int) -> bool:
    with db.begin():
        job, now = _owned(db, job_id, worker_id, generation)
        if job is None:
            return False
        db.add(ExecutionEffect(job_id=job.id, effect_payload=f"completed attempt {job.attempts}"))
        job.status = "succeeded"
        job.worker_id = None
        job.lease_expires_at = None
        job.attempt_deadline_at = None
        job.finished_at = now
        job.updated_at = now
        return True


def fail_job(db: Session, job_id: uuid.UUID, worker_id: str, generation: int, error: str) -> bool:
    with db.begin():
        job, now = _owned(db, job_id, worker_id, generation)
        if job is None:
            return False
        job.status = "failed" if job.attempts >= settings.max_attempts else "queued"
        job.error = error[:1000]
        job.worker_id = None
        job.lease_expires_at = None
        job.attempt_deadline_at = None
        job.finished_at = now if job.status == "failed" else None
        job.updated_at = now
        return True


def recover_expired(db: Session, batch_size: int = 100) -> int:
    with db.begin():
        jobs = list(db.scalars(select(Job).where(Job.status == "running", Job.attempts >= settings.max_attempts,
                                                 Job.lease_expires_at <= func.clock_timestamp())
                               .order_by(Job.lease_expires_at).limit(batch_size).with_for_update(skip_locked=True)))
        for job in jobs:
            now = db.scalar(select(func.clock_timestamp()))
            if job.lease_expires_at and job.lease_expires_at <= now:
                job.status = "failed"
                job.error = "attempt limit reached after lease expiry"
                job.worker_id = None
                job.lease_expires_at = None
                job.attempt_deadline_at = None
                job.finished_at = now
                job.updated_at = now
        return len(jobs)
