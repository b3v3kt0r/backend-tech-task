import csv
import json
import uuid
import sys

from datetime import datetime
from sqlalchemy.orm import Session
from app.db.engine import SessionLocal
from app.db.models import Event


def import_events(csv_path: str):
    """
    Import historical events from a CSV file into the database.

    Args:
        csv_path (str): Path to the CSV file with columns:
                        event_id, occurred_at, user_id, event_type, properties_json
    """

    db: Session = SessionLocal()
    added, skipped = 0, 0

    try:
        with open(csv_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                try:
                    event_id = uuid.UUID(row["event_id"])
                except ValueError:
                    print(f"⚠️ Invalid UUID: {row['event_id']} — skipping")
                    skipped += 1
                    continue

                if db.query(Event).filter_by(event_id=event_id).first():
                    skipped += 1
                    continue

                event = Event(
                    event_id=event_id,
                    occurred_at=datetime.fromisoformat(row["occurred_at"]),
                    user_id=row["user_id"],
                    event_type=row["event_type"],
                    properties=(
                        json.loads(row["properties_json"])
                        if row["properties_json"]
                        else {}
                    ),
                )

                db.add(event)
                added += 1

            db.commit()

        print(f"✅ Imported {added} events, skipped {skipped} duplicates.")

    except Exception as e:
        db.rollback()
        print(f"❌ Error importing events: {e}")

    finally:
        db.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m cli.import_events <path-to-csv>")
        sys.exit(1)

    csv_path = sys.argv[1]
    import_events(csv_path)
