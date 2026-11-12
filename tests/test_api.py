import threading

from fastapi.testclient import TestClient
from sqlalchemy import select

from soup import api
from soup.models import Job


def test_create_list_get_cancel_and_conflict(session_factory, monkeypatch):
    monkeypatch.setattr(api, "SessionLocal", session_factory)
    client = TestClient(api.app)
    headers = {"Idempotency-Key": "api-key"}
    created = client.post("/jobs", headers=headers, json={"duration_ms": 0})
    assert created.status_code == 201
    job_id = created.json()["id"]
    repeated = client.post("/jobs", headers=headers, json={"duration_ms": 0})
    assert repeated.status_code == 201 and repeated.json()["id"] == job_id
    conflict = client.post("/jobs", headers=headers, json={"duration_ms": 1})
    assert conflict.status_code == 409
    assert client.get(f"/jobs/{job_id}").json()["status"] == "queued"
    assert client.get("/jobs", params={"limit": 1}).json()["items"][0]["id"] == job_id
    cancelled = client.post(f"/jobs/{job_id}/cancel")
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancelled"
    assert client.post(f"/jobs/{job_id}/cancel").json()["status"] == "cancelled"


def test_pagination(session_factory, monkeypatch):
    monkeypatch.setattr(api, "SessionLocal", session_factory)
    client = TestClient(api.app)
    for index in range(3):
        assert client.post("/jobs", headers={"Idempotency-Key": f"page-{index}"}, json={"duration_ms": 0}).status_code == 201
    first = client.get("/jobs", params={"limit": 2}).json()
    assert len(first["items"]) == 2 and first["next_cursor"]
    second = client.get("/jobs", params={"limit": 2, "cursor": first["next_cursor"]}).json()
    assert len(second["items"]) == 1 and second["next_cursor"] is None
    assert not {item["id"] for item in first["items"]} & {item["id"] for item in second["items"]}


def test_concurrent_same_key_same_payload_returns_one_job(session_factory, monkeypatch):
    monkeypatch.setattr(api, "SessionLocal", session_factory)
    barrier = threading.Barrier(9)
    responses = []
    failures = []

    def post():
        try:
            client = TestClient(api.app)
            barrier.wait()
            responses.append(client.post("/jobs", headers={"Idempotency-Key": "same-payload"},
                                         json={"duration_ms": 7}))
        except Exception as exc:
            failures.append(exc)

    threads = [threading.Thread(target=post) for _ in range(8)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=10)
    assert not any(thread.is_alive() for thread in threads)
    assert not failures
    assert len(responses) == 8
    assert all(response.status_code == 201 for response in responses)
    assert len({response.json()["id"] for response in responses}) == 1
    with session_factory() as db:
        assert len(list(db.scalars(select(Job)))) == 1


def test_concurrent_same_key_different_payload_returns_409(session_factory, monkeypatch):
    monkeypatch.setattr(api, "SessionLocal", session_factory)
    barrier = threading.Barrier(9)
    responses = []
    failures = []

    def post(duration_ms):
        try:
            client = TestClient(api.app)
            barrier.wait()
            response = client.post("/jobs", headers={"Idempotency-Key": "mixed-payload"},
                                   json={"duration_ms": duration_ms})
            responses.append((duration_ms, response))
        except Exception as exc:
            failures.append(exc)

    threads = [threading.Thread(target=post, args=(index % 2,)) for index in range(8)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=10)
    assert not any(thread.is_alive() for thread in threads)
    assert not failures
    assert len(responses) == 8
    with session_factory() as db:
        jobs = list(db.scalars(select(Job)))
        assert len(jobs) == 1
        winner = jobs[0]
    assert sum(response.status_code == 201 for _, response in responses) == 4
    assert sum(response.status_code == 409 for _, response in responses) == 4
    assert all(response.status_code == (201 if requested == winner.duration_ms else 409)
               for requested, response in responses)
    assert all(response.json()["id"] == str(winner.id) for _, response in responses
               if response.status_code == 201)
