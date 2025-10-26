from datetime import date
import json
import time
from typing import List, Optional
import traceback

import duckdb
from fastapi import FastAPI, Depends, HTTPException, Query, status, Request
from sqlalchemy.orm import Session
from celery.result import AsyncResult
from slowapi.errors import RateLimitExceeded
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

import app.crud as crud
import app.schemas as schemas
from app.celery_ import celery_app
from app.db.db_depends import get_db
from app.tasks import create_events_task
from app.logger import logger


app = FastAPI()

limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.middleware("http")
async def log_exceptions_middleware(request: Request, call_next):
    """
    Middleware for logging requests, responses, exceptions, and simple metrics.
    """
    start_time = time.time()

    try:
        response = await call_next(request)
        status_code = response.status_code
    except Exception as e:
        status_code = 500
        error_message = (
            f"URL: {request.url} - Exception: {str(e)}\n{traceback.format_exc()}"
        )
        logger.error(
            json.dumps(
                {
                    "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "level": "ERROR",
                    "endpoint": str(request.url.path),
                    "method": request.method,
                    "status_code": status_code,
                    "exception": error_message,
                    "traceback": traceback.format_exc(),
                }
            )
        )
        raise e

    process_time = time.time() - start_time

    logger.info(
        json.dumps(
            {
                "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "level": "INFO",
                "endpoint": str(request.url.path),
                "method": request.method,
                "status_code": status_code,
                "process_time_sec": round(process_time, 4),
            }
        )
    )

    return response


@app.post(
    "/events/",
    response_model=schemas.TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
@limiter.limit("30/minute")
async def add_events(request: Request, events_data: List[schemas.EventCreate]):
    """
    Add multiple events into the database.

    Accepts a JSON array of event objects. Each event contains:
      - event_id (UUID)
      - occurred_at (ISO-8601 datetime)
      - user_id (int)
      - event_type (string)
      - properties (JSON object, optional)

    Ensures idempotency — duplicate event_ids will be ignored.
    """

    task = create_events_task.delay([e.model_dump() for e in events_data])

    return schemas.TaskResponse(
        message="Events accepted for processing",
        task_id=task.id,
        count=len(events_data),
    )


@app.get("/tasks/{task_id}")
def get_task(request: Request, task_id: str):
    """
    Function that allows to get tasks id, status, path to result.
    """

    task_res = AsyncResult(task_id, app=celery_app)

    if task_res.state == "SUCCESS":
        return schemas.TaskStatus(task_id=task_id, status=task_res.state)

    elif task_res.state == "FAILURE":
        return schemas.TaskStatus(task_id=task_id, status=task_res.state)

    else:
        raise HTTPException(
            status_code=404,
            detail=f"Task with id: {task_id} was not found or in progress",
        )


@app.get("/stats/dau")
@limiter.limit("60/minute")
def get_dau(
    request: Request,
    from_: date = Query(..., alias="from"),
    to: date = Query(...),
    segment: Optional[str] = Query(
        None, description="Format: 'event_type:value' or 'properties.field=value'"
    ),
    db: Session = Depends(get_db),
):
    """
    Get Daily Active Users (DAU) within a given date range.

    Query Parameters:
      - from: start date (inclusive)
      - to: end date (inclusive)
      - segment: filter events by segment (e.g., 'event_type:purchase' or 'properties.country=UA')

    Returns the count of unique user_id values per day.
    """
    filter_params = None

    if segment:
        if ":" in segment:
            key, value = segment.split(":", 1)
            if key == "event_type":
                filter_params = {"event_type": value}
        elif "=" in segment:
            key, value = segment.split("=", 1)
            if key.startswith("properties."):
                property_name = key.split(".", 1)[1]
                filter_params = {"properties": {property_name: value}}

    try:
        duck_res = crud.get_dau_duck(from_, to, filter_params)
        if duck_res:
            return duck_res
    except duckdb.CatalogException as e:
        logger.info(f"DuckDB table not found, falling back to SQL: {e}")
    except Exception as e:
        logger.warning(f"DuckDB DAU failed, fallback to SQL: {e}")

    return crud.get_dau(db, from_, to, filter_params)


@app.get("/stats/top-events")
@limiter.limit("60/minute")
def get_top_events(
    request: Request,
    from_: date = Query(..., alias="from"),
    to: date = Query(...),
    limit: int = 10,
    db: Session = Depends(get_db),
):
    """
    Get the most frequent event types within a date range.

    Query Parameters:
      - from: start date (inclusive)
      - to: end date (inclusive)
      - limit: maximum number of event types to return (default = 10)

    Returns a list of top event types with their occurrence counts.
    """

    try:
        duck_res = crud.get_top_events_duck(from_, to, limit)
        if duck_res:
            print(duck_res)
            return duck_res
    except Exception as e:
        logger.warning(f"Top-events DuckDB failed: {e}")

    return crud.get_top_events(db, from_, to, limit)


@app.get("/stats/retention")
@limiter.limit("60/minute")
def get_retention(
    request: Request, start_date: date, windows: int, db: Session = Depends(get_db)
):
    """
    Calculate simple cohort retention over time.

    Query Parameters:
      - start_date: the start of the first cohort window
      - windows: number of daily or weekly retention windows to calculate

    Returns a retention table showing how many users returned in each window.
    """

    try:
        duck_res = crud.get_retention_duck(start_date, windows)

        if duck_res:
            return duck_res
    except Exception as e:
        logger.warning(f"Retention DuckDB failed: {e}")

    return crud.get_retention(db, start_date, windows)
