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


def kill_live_workers(stop, interval, workers, worker_lock, randomizer, running_count,
                      spawn, recorder, failures, started):
    """Keep the fault injector alive through recovery, killing only during active work."""
    kills = 0
    try:
        while not stop.wait(interval):
            if running_count() == 0:
                recorder.log("kill_skipped", reason="no_running_jobs",
                             elapsed_seconds=round(time.monotonic() - started, 3))
                continue
            with worker_lock:
                live = [process for process in workers if process.poll() is None]
                if not live:
                    raise RuntimeError("all workers exited")
                victim = randomizer.choice(live)
                sent_at = time.monotonic()
                recorder.log("worker_sigkill_sent", pid=victim.pid,
                             elapsed_seconds=round(sent_at - started, 3))
                os.kill(victim.pid, signal.SIGKILL)
                victim.wait(timeout=10)
                if victim.returncode != -signal.SIGKILL:
                    raise RuntimeError(f"worker {victim.pid} exited with {victim.returncode}, not SIGKILL")
                kills += 1
                recorder.log("worker_sigkill", pid=victim.pid, returncode=victim.returncode,
                             elapsed_seconds=round(time.monotonic() - started, 3),
                             seconds_since_signal=round(time.monotonic() - sent_at, 3))
                workers.remove(victim)
                replacement = spawn("worker")
                workers.append(replacement)
                recorder.log("worker_replaced", killed_pid=victim.pid, pid=replacement.pid,
                             elapsed_seconds=round(time.monotonic() - started, 3))
    except Exception as exc:
        failures.append(f"kill loop: {exc!r}")
        recorder.log("kill_loop_failed", error=repr(exc))
    return kills


def verdict(audit_report, kill_enabled, kill_count, runtime_failures):
    configuration_failures = []
    if kill_enabled and kill_count == 0:
        configuration_failures.append("killing enabled but no worker received SIGKILL")
    invariant_failures = []
    if audit_report is not None and not audit_report["passed"]:
        invariant_failures.append("SQL/key invariant audit failed")
    if audit_report is None:
        runtime_failures = [*runtime_failures, "final invariant audit unavailable"]
    return configuration_failures, runtime_failures, invariant_failures


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
    database_url = os.getenv("CHAOS_DATABASE_URL", "postgresql+psycopg://soup:soup@localhost:5434/soup_chaos")
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
    runtime_failures = []
    request_errors = []
    configuration_failures = []
    kill_counts = []
    killer = None
    submit_seconds = None
    audit_report = None
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

    def running_count():
        with engine.connect() as connection:
            return connection.scalar(text("""
                SELECT count(*) FROM jobs
                WHERE idempotency_key LIKE :pattern AND status = 'running'
            """), {"pattern": prefix + "%"})

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
        if args.jobs < 1 or args.workers < 1 or args.submit_rps <= 0 or args.timeout <= 0:
            configuration_failures.append("jobs, workers, submit-rps and timeout must be positive")
        if args.kill_interval <= 0 or args.duration_ms < 0:
            configuration_failures.append("kill-interval must be positive and duration-ms nonnegative")
        if not (0 <= args.duplicate_rate <= 1 and 0 <= args.cancel_rate <= 1):
            configuration_failures.append("rates must be between 0 and 1")
        if urlparse(database_url).path != "/soup_chaos":
            configuration_failures.append("CHAOS_DATABASE_URL must point to the dedicated soup_chaos database")
        if not args.no_kill and os.name != "posix":
            configuration_failures.append("SIGKILL requires a POSIX host; run in Codespaces")
        if configuration_failures:
            recorder.log("configuration_failed", failures=configuration_failures)
            return 1
        from soup.db import Base
        import soup.models  # noqa: F401
        engine = create_engine(database_url, isolation_level="READ COMMITTED")
        Base.metadata.create_all(engine)
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
        if not args.no_kill:
            def kill_loop():
                kill_counts.append(kill_live_workers(
                    stop, args.kill_interval, workers, worker_lock, kill_randomizer,
                    running_count, spawn, recorder, runtime_failures, started))
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
                    request_errors.append(repr(exc))
                    runtime_failures.append(repr(exc))
            for future in concurrent.futures.as_completed(followups):
                try:
                    future.result()
                except Exception as exc:
                    request_errors.append(repr(exc))
                    runtime_failures.append(repr(exc))
        submit_seconds = time.monotonic() - submit_start
        recorder.log("submissions_finished", seconds=submit_seconds, primary_rps=args.jobs / submit_seconds,
                     request_errors=request_errors)
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            audit_report = audit(database_url, prefix, args.jobs)
            recorder.log("recovery_poll", statuses=audit_report["statuses"],
                         missing=len(audit_report["missing_keys"]))
            if audit_report["passed"]:
                break
            time.sleep(1)
    except BaseException as exc:
        runtime_failures.append(repr(exc))
        recorder.log("runtime_failed", error=repr(exc))
    finally:
        stop.set()
        if killer:
            killer.join(timeout=args.kill_interval + 10)
            if killer.is_alive():
                runtime_failures.append("kill loop did not stop")
        try:
            audit_report = audit(database_url, prefix, args.jobs)
            recorder.log("final_audit", audit=audit_report)
        except Exception as exc:
            runtime_failures.append(f"final audit: {exc!r}")
            recorder.log("final_audit_failed", error=repr(exc))
            audit_report = None
        kill_count = sum(kill_counts)
        missing_kills, runtime_failures, invariant_failures = verdict(
            audit_report, not args.no_kill, kill_count, runtime_failures)
        configuration_failures.extend(missing_kills)
        report = dict(audit_report or {})
        report.update(configuration_failures=configuration_failures,
                      runtime_failures=runtime_failures, invariant_failures=invariant_failures,
                      request_errors=request_errors, worker_kills=kill_count,
                      elapsed_seconds=round(time.monotonic() - started, 2),
                      target_post_rps=args.submit_rps,
                      primary_post_rps=round(args.jobs / submit_seconds, 2) if submit_seconds else None)
        report["target_post_rps_met"] = (report["primary_post_rps"] is not None and
                                         report["primary_post_rps"] >= args.submit_rps)
        report["passed"] = not (configuration_failures or runtime_failures or invariant_failures)
        (run_dir / "audit.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        recorder.log("run_finished", report=report)
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
        if 'engine' in locals():
            engine.dispose()
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
