import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from app.crud import create_events, get_dau
from app.db.engine import SessionLocal, Base, engine
from app.schemas import EventCreate


def run_benchmark(num_events: int = 100_000, num_users: int = 1000, days: int = 30):
    print(
        f"Starting benchmark: {num_events} events, {num_users} users, last {days} days"
    )

    Base.metadata.create_all(bind=engine)
    session = SessionLocal()

    now = datetime.now(timezone.utc)
    events = []
    for i in range(num_events):
        events.append(
            EventCreate(
                event_id=uuid4(),
                user_id=f"user{i % num_users}",
                event_type="login" if i % 2 == 0 else "purchase",
                occurred_at=now - timedelta(days=i % days),
                properties={},
            )
        )

    start_insert = time.time()
    create_events(session, events)
    end_insert = time.time()
    print(f"Inserted {num_events} events in {end_insert - start_insert:.2f} sec")

    start_dau = time.time()
    dau = get_dau(session, now.date() - timedelta(days=1), now.date())
    end_dau = time.time()
    print(f"DAU computed in {end_dau - start_dau:.2f} sec")
    print(f"DAU result: {dau}")

    session.close()


if __name__ == "__main__":
    run_benchmark()
