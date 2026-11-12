import base64
import binascii
import json
import logging
import time
import uuid
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import text

from soup.db import SessionLocal, engine, init_db
from soup import queue


class JobCreate(BaseModel):
    duration_ms: int = Field(default=1000, ge=0, le=120000)
    should_fail: bool = False


def public(job):
    return {"id": str(job.id), "status": job.status, "attempts": job.attempts,
            "lease_generation": job.lease_generation, "created_at": job.created_at,
            "updated_at": job.updated_at, "finished_at": job.finished_at, "error": job.error}


def encode_cursor(job) -> str:
    value = json.dumps([job.created_at.isoformat(), str(job.id)], separators=(",", ":"))
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


def decode_cursor(value: str) -> tuple[datetime, uuid.UUID]:
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        timestamp, job_id = json.loads(raw)
        parsed = datetime.fromisoformat(timestamp)
        if parsed.tzinfo is None:
            raise ValueError("timezone required")
        return parsed, uuid.UUID(job_id)
    except (ValueError, TypeError, KeyError, IndexError, UnicodeDecodeError, binascii.Error):
        raise HTTPException(422, "invalid cursor") from None


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(lifespan=lifespan)
logger = logging.getLogger("soup.api")


@app.middleware("http")
async def access_log(request: Request, call_next):
    started = time.monotonic()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        logger.info(json.dumps({"event": "http_request", "method": request.method, "path": request.url.path,
                                "status": status, "duration_ms": round((time.monotonic() - started) * 1000, 2)}))


@app.get("/health")
def health():
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.post("/jobs", status_code=201)
def create_job(body: JobCreate, idempotency_key: str = Header(min_length=1, max_length=255, alias="Idempotency-Key")):
    with SessionLocal() as db:
        job, same = queue.create_job(db, idempotency_key, body.duration_ms, body.should_fail)
        if not same:
            raise HTTPException(409, "idempotency key already used for another request body")
        return public(job)


@app.get("/jobs/{job_id}")
def get_job(job_id: uuid.UUID):
    with SessionLocal() as db:
        job = queue.get_job(db, job_id)
        if not job:
            raise HTTPException(404, "job not found")
        return public(job)


@app.get("/jobs")
def list_jobs(limit: int = Query(default=50, ge=1, le=100), cursor: str | None = None):
    parsed = decode_cursor(cursor) if cursor else None
    with SessionLocal() as db:
        rows = queue.list_jobs(db, limit, parsed)
        page = rows[:limit]
        return {"items": [public(row) for row in page], "next_cursor": encode_cursor(page[-1]) if len(rows) > limit else None}


@app.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: uuid.UUID):
    with SessionLocal() as db:
        job = queue.cancel_job(db, job_id)
        if not job:
            raise HTTPException(404, "job not found")
        return public(job)
