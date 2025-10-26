from datetime import datetime, date
from uuid import UUID
from typing import Any, Optional

from pydantic import BaseModel


class TaskResponse(BaseModel):

    message: str
    task_id: str
    count: int


class TaskStatus(BaseModel):

    task_id: str
    status: str


class EventBase(BaseModel):
    event_id: UUID
    occurred_at: datetime
    user_id: int
    event_type: str
    properties: Optional[dict[str, Any]] = None


class EventCreate(EventBase):
    pass


class EventResponse(EventBase):
    model_config = {"from_attributes": True}


class DAUItem(BaseModel):
    date: date
    unique_users: int


class DAUResponse(BaseModel):
    results: list[DAUItem]


class TopEventItem(BaseModel):
    event_type: str
    count: int


class TopEventsResponse(BaseModel):
    results: list[TopEventItem]


class RetentionItem(BaseModel):
    cohort_date: date
    day_0: int
    day_1: int
    day_2: int
    day_3: int


class RetentionResponse(BaseModel):
    results: list[RetentionItem]
