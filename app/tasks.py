import json
import pytz

from celery import shared_task

from app import schemas, crud
from app.db.db_depends import get_db
from app.db.engine import SessionLocal
from app.db.duck import write_analytics, get_duck_conn
from app.db.models import Event


@shared_task(autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def create_events_task(events_data: list[dict]):
    """
    Celery task for creating events asynchronously.
    Ignores duplicates (idempotent behavior).
    """

    db = SessionLocal()
    try:
        events = [schemas.EventCreate(**e) for e in events_data]
        created = crud.create_events(db, events)
        db.commit()
        return [str(ev.event_id) for ev in created]
    finally:
        db.close()


@shared_task(autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def sync_events_to_duck():
    """
    Sync Events from SQL to DuckDB using batch insert.
    """

    conn = get_duck_conn()
    conn.execute("PRAGMA threads=1;")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            user_id TEXT,
            occurred_at TIMESTAMP,
            event_type TEXT,
            properties JSON
        )
    """
    )

    last_ts = conn.execute(
        "SELECT COALESCE(MAX(occurred_at), TIMESTAMP '1970-01-01') FROM events"
    ).fetchone()[0]

    db = next(get_db())
    try:
        new_events = (
            db.query(Event)
            .filter(Event.occurred_at > last_ts)
            .order_by(Event.occurred_at)
            .all()
        )

        if not new_events:
            return "No new events"

        data = [
            (
                str(ev.user_id),
                ev.occurred_at.astimezone(pytz.UTC).replace(tzinfo=None),
                ev.event_type,
                json.dumps(ev.properties) if ev.properties else None,
            )
            for ev in new_events
        ]

        sql = "INSERT INTO events (user_id, occurred_at, event_type, properties) VALUES (?, ?, ?, ?)"
        write_analytics(conn, sql, data)

        return f"Synced {len(new_events)} events"

    finally:
        conn.close()
        db.close()
