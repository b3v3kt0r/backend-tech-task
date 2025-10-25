from datetime import date, datetime, time, timedelta

from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.db import models
from app.db.duck import query_analytics
import app.schemas as schemas


def create_events(db: Session, events: List[schemas.EventCreate]):
    created = []
    for e in events:
        exists = (
            db.query(models.Event).filter(models.Event.event_id == e.event_id).first()
        )
        if not exists:
            event = models.Event(**e.model_dump())
            db.add(event)
            created.append(event)
    db.commit()
    return created


def get_dau(db: Session, from_date: date, to_date: date, filter_params=None):
    from_dt = datetime.combine(from_date, time.min)
    to_dt = datetime.combine(to_date, time.max)

    query = db.query(
        func.date(models.Event.occurred_at).label("date"),
        func.count(func.distinct(models.Event.user_id)).label("dau"),
    ).filter(
        models.Event.occurred_at >= from_dt,
        models.Event.occurred_at <= to_dt,
    )

    if filter_params:
        if "event_type" in filter_params:
            query = query.filter(models.Event.event_type == filter_params["event_type"])
        if "properties" in filter_params:
            for key, value in filter_params["properties"].items():
                query = query.filter(models.Event.properties[key].astext == value)

    query = query.group_by(func.date(models.Event.occurred_at)).order_by("date")
    result = query.all()
    return [{"date": r.date, "dau": r.dau} for r in result]


def get_dau_duck(from_: date, to: date, filter_params: Optional[Dict]) -> List[Dict]:
    """
    Get Daily Active Users from DuckDB.
    Note: No SQLAlchemy db parameter needed for DuckDB queries.
    """
    sql = """
        SELECT CAST(occurred_at AS DATE) AS day,
               COUNT(DISTINCT user_id) AS dau
        FROM events
        WHERE CAST(occurred_at AS DATE) BETWEEN ? AND ?
    """

    params = [from_, to]

    if filter_params:
        if "event_type" in filter_params:
            sql += " AND event_type = ?"
            params.append(filter_params["event_type"])

        if "properties" in filter_params:
            for key, value in filter_params["properties"].items():
                sql += " AND json_extract(properties, ?) = ?"
                params.append(f"$.{key}")
                params.append(value)

    sql += " GROUP BY day ORDER BY day;"

    rows = query_analytics(sql, params)

    return [{"day": row[0], "count": row[1]} for row in rows]


def get_top_events(db: Session, from_date: date, to_date: date, limit: int = 10):
    from_dt = datetime.combine(from_date, time.min)
    to_dt = datetime.combine(to_date, time.max)

    result = (
        db.query(
            models.Event.event_type, func.count(models.Event.event_id).label("count")
        )
        .filter(models.Event.occurred_at >= from_dt, models.Event.occurred_at <= to_dt)
        .group_by(models.Event.event_type)
        .order_by(func.count(models.Event.event_id).desc())
        .limit(limit)
        .all()
    )
    return [{"event_type": r.event_type, "count": r.count} for r in result]


def get_top_events_duck(from_: date, to: date, limit: int = 10):
    sql = """
        SELECT event_type, COUNT(*) AS cnt
        FROM events
        WHERE CAST(occurred_at AS DATE) BETWEEN ? AND ?
        GROUP BY event_type
        ORDER BY cnt DESC
        LIMIT ?;
    """
    rows = query_analytics(sql, [from_, to, limit])
    return [{"event_type": r[0], "count": r[1]} for r in rows]


def get_retention(db: Session, start_date: date, windows: int = 3):
    cohorts = []
    for i in range(windows):
        window_start = start_date + timedelta(days=i)
        window_end = window_start + timedelta(days=1)
        active_users = (
            db.query(func.count(func.distinct(models.Event.user_id)))
            .filter(
                models.Event.occurred_at >= window_start,
                models.Event.occurred_at < window_end,
            )
            .scalar()
        )
        cohorts.append({"window": i + 1, "active_users": active_users})
    return cohorts


def get_retention_duck(start_date: date, windows: int = 3):
    sql = """
        SELECT win, COUNT(DISTINCT user_id) AS active_users
        FROM (
            SELECT 
                user_id,
                CAST(julian(CAST(occurred_at AS DATE)) - julian(CAST(? AS DATE)) AS INT) AS win
            FROM events
        )
        WHERE win >= 0 AND win < ?
        GROUP BY win
        ORDER BY win;
    """

    rows = query_analytics(sql, [start_date, windows])
    return [{"window": r[0] + 1, "active_users": r[1]} for r in rows]
