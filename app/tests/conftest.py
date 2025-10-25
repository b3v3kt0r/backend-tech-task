import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from celery import Celery

from app.tasks import create_events_task
from app.db.engine import Base


@pytest.fixture
def db_session():
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
