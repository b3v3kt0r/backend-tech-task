import uuid

from sqlalchemy import Column, String, DateTime, JSON, Integer
from sqlalchemy.dialects.postgresql import UUID

from app.db.engine import Base


class Event(Base):
    """
    Class for Event handling.
    """

    __tablename__ = "events"

    event_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        unique=True,
        index=True,
    )
    occurred_at = Column(DateTime(timezone=True))
    user_id = Column(Integer)
    event_type = Column(String)
    properties = Column(JSON)
