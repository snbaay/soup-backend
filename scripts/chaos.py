"""Process-level chaos run against a dedicated PostgreSQL database.

Run on the host after `docker compose --profile chaos up -d chaos-db`.
The API and workers are child processes so SIGKILL and replacement are explicit.
"""
import argparse
import concurrent.futures
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import create_engine, text


ROOT = Path(__file__).resolve().parents[1]


def audit(database_url: str, prefix: str, expected: int) -> dict:
    """SQL reconciliation; the Python key-set check catches gaps hidden by counts."""
    engine = create_engine(database_url, isolation_level="READ COMMITTED")
    try:
        with engine.connect() as connection:
            rows = connection.execute(text("""
                SELECT j.idempotency_key, j.status, j.id, count(e.job_id) AS effects
                FROM jobs j LEFT JOIN execution_effects e ON e.job_id = j.id
                WHERE j.idempotency_key LIKE :pattern
                GROUP BY j.id, j.idempotency_key, j.status
            """), {"pattern": prefix + "%"}).all()
            duplicate_effects = connection.scalar(text("""
                SELECT count(*) FROM (
                  SELECT e.job_id FROM execution_effects e JOIN jobs j ON j.id=e.job_id
                  WHERE j.idempotency_key LIKE :pattern GROUP BY e.job_id HAVING count(*) > 1
                ) duplicates
            """), {"pattern": prefix + "%"})
            invalid = connection.scalar(text("""
                SELECT count(*) FROM jobs j
                WHERE j.idempotency_key LIKE :pattern AND (
                  (j.status = 'succeeded' AND NOT EXISTS
                    (SELECT 1 FROM execution_effects e WHERE e.job_id=j.id)) OR
                  (j.status <> 'succeeded' AND EXISTS
                    (SELECT 1 FROM execution_effects e WHERE e.job_id=j.id)) OR
                  (j.status IN ('queued','running') AND j.finished_at IS NOT NULL) OR
                  (j.status IN ('succeeded','failed','cancelled') AND j.finished_at IS NULL)
                )
            """), {"pattern": prefix + "%"})
        found = {row.idempotency_key for row in rows}
        wanted = {f"{prefix}{i}" for i in range(expected)}
        statuses = {status: sum(row.status == status for row in rows)
                    for status in ("queued", "running", "succeeded", "failed", "cancelled")}
        result = {"expected": expected, "found": len(rows), "missing_keys": sorted(wanted - found),
                  "unexpected_keys": sorted(found - wanted), "statuses": statuses,
                  "duplicate_effects": duplicate_effects, "invalid_terminal_effect_or_timestamp": invalid,
                  "effect_count": sum(row.effects for row in rows)}
        result["passed"] = (len(rows) == expected and not result["missing_keys"] and
                            not result["unexpected_keys"] and not duplicate_effects and not invalid and
                            statuses["queued"] == 0 and statuses["running"] == 0)
        return result
    finally:
        engine.dispose()


class Recorder:
    def __init__(self, path: Path, seed: int):
        self.file = path.open("x", encoding="utf-8")
        self.lock = threading.Lock()
        self.seed = seed

    def log(self, event: str, **fields):
        line = json.dumps({"timestamp": datetime.now(timezone.utc).isoformat(),
                           "seed": self.seed, "event": event, **fields}, default=str)
        with self.lock:
            self.file.write(line + "\n")
            self.file.flush()
        print(line, flush=True)

    def close(self):
        self.file.close()


def request(base: str, method: str, path: str, payload=None, key=None):
    headers = {"Content-Type": "application/json"}
    if key is not None:
        headers["Idempotency-Key"] = key
    data = json.dumps(payload).encode() if payload is not None else None
    started = time.monotonic()
    try:
        with urllib.request.urlopen(urllib.request.Request(base + path, data=data, headers=headers,
                                                          method=method), timeout=15) as response:
            body = json.load(response)
            code = response.status
    except urllib.error.HTTPError as error:
        code = error.code
        body = error.read().decode(errors="replace")
    return code, body, round((time.monotonic() - started) * 1000, 2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=10000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--seed", type=int, default=20261112)
    parser.add_argument("--kill-interval", type=float, default=2.0)
    parser.add_argument("--duplicate-rate", type=float, default=0.1)
    parser.add_argument("--cancel-rate", type=float, default=0.1)
    parser.add_argument("--submit-rps", type=float, default=100)
    parser.add_argument("--duration-ms", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--no-kill", action="store_true")
    args = parser.parse_args()
    if args.jobs < 1 or args.workers < 1 or args.submit_rps <= 0 or args.timeout <= 0:
        parser.error("jobs, workers, submit-rps and timeout must be positive")
    if not (0 <= args.duplicate_rate <= 1 and 0 <= args.cancel_rate <= 1):
        parser.error("rates must be between 0 and 1")
    database_url = os.getenv("CHAOS_DATABASE_URL", "postgresql+psycopg://soup:soup@localhost:5434/soup_chaos")
    if urlparse(database_url).path != "/soup_chaos":
        parser.error("CHAOS_DATABASE_URL must point to the dedicated soup_chaos database")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
    run_dir = ROOT / "artifacts" / "runs" / f"chaos-{run_id}"
    run_dir.mkdir(parents=True, exist_ok=False)
    recorder = Recorder(run_dir / "events.jsonl", args.seed)
    prefix = f"chaos-{run_id}-"
    env = dict(os.environ, DATABASE_URL=database_url)
    env.setdefault("LEASE_SECONDS", "3")
    env.setdefault("ATTEMPT_SECONDS", "15")
    env.setdefault("HEARTBEAT_SECONDS", "0.5")
    env.setdefault("POLL_SECONDS", "0.05")
    api = None
    workers = []
    handles = []
    stop = threading.Event()
    errors = []
    kill_count = 0
    base = f"http://127.0.0.1:{args.port}"
    kill_randomizer = random.Random(args.seed)

    def spawn(kind):
        number = len(handles)
        output = (run_dir / f"{kind}-{number}.log").open("x", encoding="utf-8")
        handles.append(output)
        command = ([sys.executable, "-m", "uvicorn", "soup.api:app", "--host", "127.0.0.1",
                    "--port", str(args.port)] if kind == "api" else [sys.executable, "-m", "soup.worker"])
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT)
        recorder.log("process_started", kind=kind, pid=process.pid, output=output.name)
        return process

    def kill_loop():
        nonlocal kill_count
        try:
            while not stop.wait(args.kill_interval):
                with worker_lock:
                    live = [p for p in workers if p.poll() is None]
                    if not live:
                        raise RuntimeError("all workers exited")
                    victim = kill_randomizer.choice(live)
                    if os.name == "posix":
                        os.kill(victim.pid, signal.SIGKILL)
                    else:
                        victim.kill()  # Windows force termination; POSIX uses literal SIGKILL.
                    victim.wait(timeout=10)
                    kill_count += 1
                    recorder.log("worker_sigkill", pid=victim.pid, returncode=victim.returncode)
                    workers.remove(victim)
                    workers.append(spawn("worker"))
        except Exception as exc:
            errors.append(f"kill loop: {exc!r}")
            recorder.log("kill_loop_failed", error=repr(exc))

    def post(index):
        key = f"{prefix}{index}"
        code, body, latency = request(base, "POST", "/jobs", {"duration_ms": args.duration_ms}, key)
        recorder.log("post", key=key, status=code, latency_ms=latency, body=body)
        if code != 201:
            raise RuntimeError(f"create {key}: HTTP {code}")
        choice = random.Random(args.seed + index)
        with followup_lock:
            if choice.random() < args.duplicate_rate:
                followups.append(pool.submit(followup, "duplicate", index, body["id"]))
            if choice.random() < args.cancel_rate:
                followups.append(pool.submit(followup, "cancel", index, body["id"]))
        return index, body["id"]

    def followup(kind, index, job_id):
        key = f"{prefix}{index}"
        if kind == "duplicate":
            code, body, latency = request(base, "POST", "/jobs", {"duration_ms": args.duration_ms}, key)
            valid = code == 201 and body["id"] == job_id
        else:
            code, body, latency = request(base, "POST", f"/jobs/{job_id}/cancel")
            valid = code == 200 and body["id"] == job_id
        recorder.log(kind, key=key, status=code, latency_ms=latency, valid=valid, body=body)
        if not valid:
            raise RuntimeError(f"{kind} {key}: HTTP {code}, inconsistent id")

    worker_lock = threading.Lock()
    followup_lock = threading.Lock()
    followups = []
    started = time.monotonic()
    recorder.log("run_started", arguments=vars(args), database=database_url.split("@")[ -1], prefix=prefix)
    try:
        from soup.db import Base
        import soup.models  # noqa: F401
        engine = create_engine(database_url, isolation_level="READ COMMITTED")
        Base.metadata.create_all(engine)
        engine.dispose()
        api = spawn("api")
        for _ in range(args.workers):
            workers.append(spawn("worker"))
        for _ in range(100):
            if api.poll() is not None:
                raise RuntimeError("API exited during startup")
            try:
                if request(base, "GET", "/health")[0] == 200:
                    break
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.1)
        else:
            raise TimeoutError("API health check timed out")
        killer = None
        if not args.no_kill:
            killer = threading.Thread(target=kill_loop, daemon=True)
            killer.start()
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
            submit_start = time.monotonic()
            for index in range(args.jobs):
                due = submit_start + index / args.submit_rps
                if due > time.monotonic():
                    time.sleep(due - time.monotonic())
                futures.append(pool.submit(post, index))
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    errors.append(repr(exc))
            for future in concurrent.futures.as_completed(followups):
                try:
                    future.result()
                except Exception as exc:
                    errors.append(repr(exc))
        submit_seconds = time.monotonic() - submit_start
        stop.set()
        if killer:
            killer.join(timeout=args.kill_interval + 10)
        if not args.no_kill and submit_seconds >= args.kill_interval and kill_count == 0:
            errors.append("no worker kill occurred")
        recorder.log("submissions_finished", seconds=submit_seconds, primary_rps=args.jobs / submit_seconds,
                     kills=kill_count, request_errors=errors)
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            report = audit(database_url, prefix, args.jobs)
            recorder.log("recovery_poll", statuses=report["statuses"], missing=len(report["missing_keys"]))
            if report["passed"]:
                break
            time.sleep(1)
        report = audit(database_url, prefix, args.jobs)
        report["request_errors"] = errors
        report["passed"] = report["passed"] and not errors
        report["primary_post_rps"] = round(args.jobs / submit_seconds, 2)
        report["target_post_rps"] = args.submit_rps
        report["target_post_rps_met"] = report["primary_post_rps"] >= args.submit_rps
        report["worker_kills"] = kill_count
        report["elapsed_seconds"] = round(time.monotonic() - started, 2)
        (run_dir / "audit.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        recorder.log("run_finished", report=report)
        return 0 if report["passed"] else 1
    except BaseException as exc:
        recorder.log("run_failed", error=repr(exc))
        raise
    finally:
        stop.set()
        for process in ([api] if api else []) + workers:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            recorder.log("process_stopped", pid=process.pid, returncode=process.returncode)
        for handle in handles:
            handle.close()
        recorder.close()


if __name__ == "__main__":
    sys.exit(main())
