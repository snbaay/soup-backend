"""Focused process lifecycle checks; these do not require PostgreSQL."""
import random
import signal
import threading
import time
from unittest.mock import patch

from scripts.chaos import kill_live_workers, verdict


class FakeProcess:
    def __init__(self, pid):
        self.pid = pid
        self.returncode = None

    def poll(self):
        return self.returncode

    def wait(self, timeout):
        assert timeout == 10
        return self.returncode


class FakeRecorder:
    def __init__(self):
        self.events = []

    def log(self, event, **fields):
        self.events.append((event, fields))


def test_kill_replaces_live_worker_and_records_real_sigkill():
    workers = [FakeProcess(101)]
    recorder = FakeRecorder()
    failures = []
    waits = iter([False, True])
    stop = threading.Event()
    stop.wait = lambda interval: next(waits)

    def send_signal(pid, sig):
        assert (pid, sig) == (101, signal.SIGKILL)
        workers[0].returncode = -signal.SIGKILL

    with patch("scripts.chaos.os.kill", side_effect=send_signal):
        count = kill_live_workers(stop, 2, workers, threading.Lock(), random.Random(1),
                                  lambda: 1, lambda kind: FakeProcess(202), recorder,
                                  failures, time.monotonic())

    assert count == 1
    assert not failures
    assert [worker.pid for worker in workers] == [202]
    assert [event for event, _ in recorder.events] == [
        "worker_sigkill_sent", "worker_sigkill", "worker_replaced"]
    assert recorder.events[1][1]["returncode"] == -signal.SIGKILL
    assert recorder.events[2][1]["killed_pid"] == 101


def test_enabled_killing_without_observed_kill_fails_verdict():
    clean_audit = {"passed": True}
    configuration, runtime, invariants = verdict(clean_audit, True, 0, [])
    assert configuration == ["killing enabled but no worker received SIGKILL"]
    assert not runtime and not invariants
    assert verdict(clean_audit, False, 0, []) == ([], [], [])


def test_invariant_and_runtime_failures_remain_distinct():
    configuration, runtime, invariants = verdict({"passed": False}, True, 1, ["POST failed"])
    assert not configuration
    assert runtime == ["POST failed"]
    assert invariants == ["SQL/key invariant audit failed"]
