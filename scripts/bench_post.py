"""POST-only, open-loop HTTP benchmark. Start one API and no workers separately.

Each run keeps every request result in a new artifacts/runs directory. This
measures creation over HTTP, including the client's connection and JSON costs.
"""
import argparse
import concurrent.futures
import http.client
import json
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
LOCAL = threading.local()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[max(0, int(len(ordered) * fraction + 0.999999) - 1)], 2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8002")
    parser.add_argument("--requests", type=int, default=3000)
    parser.add_argument("--target-rps", type=float, default=100)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("--duration-ms", type=int, default=0)
    args = parser.parse_args()
    if args.requests < 1 or args.target_rps <= 0 or args.concurrency < 1 or args.timeout <= 0:
        parser.error("requests, target-rps, concurrency and timeout must be positive")
    if not 0 <= args.duration_ms <= 120000:
        parser.error("duration-ms must be between 0 and 120000")
    url = urlsplit(args.url)
    if url.scheme != "http" or not url.hostname or url.path not in ("", "/") or url.query:
        parser.error("--url must be an HTTP origin without a path or query")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
    run_dir = ROOT / "artifacts" / "runs" / f"post-bench-{run_id}"
    run_dir.mkdir(parents=True, exist_ok=False)
    prefix = f"post-bench-{run_id}-"
    lock = threading.Lock()
    in_flight = 0
    max_in_flight = 0
    results = []
    body = json.dumps({"duration_ms": args.duration_ms}, separators=(",", ":"))
    origin = f"{url.hostname}:{url.port or 80}"

    with (run_dir / "requests.jsonl").open("x", encoding="utf-8") as raw:
        def record(row):
            with lock:
                raw.write(json.dumps(row, separators=(",", ":")) + "\n")
                raw.flush()
                results.append(row)

        def post(index, due):
            nonlocal in_flight, max_in_flight
            started = time.monotonic()
            with lock:
                in_flight += 1
                max_in_flight = max(max_in_flight, in_flight)
            connection = getattr(LOCAL, "connection", None)
            if connection is None:
                connection = http.client.HTTPConnection(url.hostname, url.port or 80, timeout=args.timeout)
                LOCAL.connection = connection
            new_connection = connection.sock is None
            row = {"timestamp": utc_now(), "index": index, "status": None,
                   "error": None, "dispatch_lag_ms": round((started - due) * 1000, 2),
                   "new_connection": new_connection}
            try:
                connection.request("POST", "/jobs", body=body,
                                   headers={"Content-Type": "application/json",
                                            "Idempotency-Key": f"{prefix}{index}"})
                response = connection.getresponse()
                payload = response.read()
                row["status"] = response.status
                if response.status == 201:
                    parsed = json.loads(payload)
                    row["job_id"] = parsed["id"]
                else:
                    row["error"] = payload.decode(errors="replace")[:500]
            except Exception as exc:
                row["error"] = repr(exc)
                connection.close()
            finally:
                row["latency_ms"] = round((time.monotonic() - started) * 1000, 2)
                row["finished_monotonic"] = time.monotonic()
                with lock:
                    in_flight -= 1
                record(row)

        started = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = []
            for index in range(args.requests):
                due = started + index / args.target_rps
                delay = due - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                futures.append(pool.submit(post, index, due))
            for future in concurrent.futures.as_completed(futures):
                future.result()
        elapsed = max(row["finished_monotonic"] for row in results) - started

    success = [row for row in results if row["status"] == 201 and row["error"] is None]
    latencies = [row["latency_ms"] for row in success]
    report = {
        "timestamp": utc_now(), "run_id": run_id, "url": args.url,
        "requests": args.requests, "duration_ms": args.duration_ms,
        "target_rps": args.target_rps, "configured_concurrency": args.concurrency,
        "max_in_flight": max_in_flight, "timeout_seconds": args.timeout,
        "elapsed_seconds": round(elapsed, 3),
        "achieved_primary_rps": round(len(success) / elapsed, 2),
        "attempted_rps": round(len(results) / elapsed, 2),
        "succeeded": len(success), "errors": len(results) - len(success),
        "status_counts": {str(code): sum(row["status"] == code for row in results)
                          for code in sorted({row["status"] for row in results if row["status"] is not None})},
        "new_connections": sum(row["new_connection"] for row in results),
        "latency_ms_success": {name: percentile(latencies, fraction)
                               for name, fraction in (("p50", .50), ("p95", .95), ("p99", .99))},
        "dispatch_lag_ms": {name: percentile([row["dispatch_lag_ms"] for row in results], fraction)
                            for name, fraction in (("p50", .50), ("p95", .95), ("p99", .99))},
        "raw_requests": str(run_dir / "requests.jsonl"),
    }
    (run_dir / "summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2), flush=True)
    return 0 if report["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
