import uuid

import pytest
from fastapi import HTTPException

from soup.api import decode_cursor, encode_cursor
from soup.queue import body_hash


def test_hash_stable_and_sensitive():
    assert body_hash(100, False) == body_hash(100, False)
    assert body_hash(100, False) != body_hash(101, False)
    assert body_hash(100, False) != body_hash(100, True)


def test_cursor_round_trip():
    from datetime import datetime, timezone
    job = type("Job", (), {"created_at": datetime.now(timezone.utc), "id": uuid.uuid4()})()
    assert decode_cursor(encode_cursor(job)) == (job.created_at, job.id)
    with pytest.raises(HTTPException):
        decode_cursor("broken")
