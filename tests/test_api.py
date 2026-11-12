from fastapi.testclient import TestClient

from soup import api


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
