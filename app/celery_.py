from celery import Celery
from celery.schedules import crontab

from app.tasks import create_events_task


celery_app = Celery(
    "events_service", broker="redis://redis:6379", backend="redis://redis:6379"
)

celery_app.conf.beat_schedule = {
    "sync-duckdb-every-minute": {
        "task": "app.tasks.sync_events_to_duck",
        "schedule": crontab(minute="*/1"),
    },
}

celery_app.autodiscover_tasks(packages=["app"])
