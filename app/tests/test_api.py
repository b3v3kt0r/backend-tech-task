from datetime import datetime
from uuid import uuid4

from app.crud import create_events, get_dau, get_top_events
from app.schemas import EventCreate


def test_create_events_idempotent(db_session):
    test_uuid = uuid4()
    event_data = EventCreate(
        event_id=test_uuid,
        occurred_at=datetime.now(),
        user_id=1,
        event_type="login",
        properties={"foo": "bar"},
    )

    created = create_events(db_session, [event_data])
    assert len(created) == 1

    created_again = create_events(db_session, [event_data])
    assert len(created_again) == 0


def test_get_dau_and_top_events(db_session):
    now = datetime.now()
    events = [
        EventCreate(
            event_id=uuid4(),
            occurred_at=now,
            user_id=2,
            event_type="login",
            properties={},
        ),
        EventCreate(
            event_id=uuid4(),
            occurred_at=now,
            user_id=3,
            event_type="purchase",
            properties={},
        ),
        EventCreate(
            event_id=uuid4(),
            occurred_at=now,
            user_id=2,
            event_type="login",
            properties={},
        ),
    ]
    create_events(db_session, events)

    dau = get_dau(
        db_session, events[0].occurred_at.date(), events[0].occurred_at.date()
    )
    assert dau[0]["dau"] == 2

    top_events = get_top_events(
        db_session, events[0].occurred_at.date(), events[0].occurred_at.date()
    )
    assert top_events[0]["event_type"] == "login"


def test_ingest_to_dau(db_session):
    events = [
        EventCreate(
            event_id=uuid4(),
            occurred_at=datetime.now(),
            user_id=1,
            event_type="login",
            properties={},
        ),
        EventCreate(
            event_id=uuid4(),
            occurred_at=datetime.now(),
            user_id=2,
            event_type="login",
            properties={},
        ),
        EventCreate(
            event_id=uuid4(),
            occurred_at=datetime.now(),
            user_id=1,
            event_type="purchase",
            properties={},
        ),
    ]

    create_events(db_session, events)

    event_date = events[0].occurred_at.date()
    dau = get_dau(db_session, event_date, event_date)
    assert len(dau) > 0, "DAU query returned empty results"
    assert dau[0]["dau"] == 2
