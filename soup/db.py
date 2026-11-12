from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "postgresql+psycopg://soup:soup@localhost:5432/soup"
    lease_seconds: int = Field(default=10, gt=0)
    attempt_seconds: int = Field(default=60, gt=0)
    max_attempts: int = Field(default=3, gt=0)
    heartbeat_seconds: float = Field(default=2.0, gt=0)
    poll_seconds: float = Field(default=0.2, gt=0)
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
engine = create_engine(settings.database_url, isolation_level="READ COMMITTED", pool_pre_ping=True)
SessionLocal = sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


def init_db() -> None:
    from soup.models import Job, ExecutionEffect  # noqa: F401
    Base.metadata.create_all(engine)
