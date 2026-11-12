import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from soup.db import Base
from soup.models import Job, ExecutionEffect  # noqa: F401


@pytest.fixture
def session_factory():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("set TEST_DATABASE_URL to a dedicated PostgreSQL test database")
    if not url.rsplit("/", 1)[-1] == "soup_test":
        pytest.fail("TEST_DATABASE_URL must point to soup_test; tests recreate its tables")
    engine = create_engine(url, isolation_level="READ COMMITTED")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield sessionmaker(engine, expire_on_commit=False)
    Base.metadata.drop_all(engine)
    engine.dispose()
