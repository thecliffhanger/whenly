"""Data models for whenly."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


def _utcnow() -> datetime:
    return datetime.utcnow()


def _uuid() -> str:
    return uuid.uuid4().hex


class ScheduleType(str, Enum):
    INTERVAL = "interval"
    CRON = "cron"
    ONCE = "once"


class MissedPolicy(str, Enum):
    RUN_ONCE = "run_once"
    RUN_ALL = "run_all"
    SKIP = "skip"


class JobStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    RUNNING = "running"
    PENDING = "pending"


@dataclass
class Job:
    name: str
    func_path: str
    schedule_type: ScheduleType
    interval_seconds: int | float | None = None
    cron_expr: str | None = None
    scheduled_for: datetime | None = None
    missed_policy: MissedPolicy = MissedPolicy.RUN_ONCE
    max_concurrent: int = 1
    timeout_seconds: int | None = None
    enabled: bool = True
    id: str = field(default_factory=_uuid)
    next_run_at: datetime | None = None
    last_run_at: datetime | None = None
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)
    # Runtime only — not persisted
    func: Any = field(default=None, repr=False, compare=False)


@dataclass
class JobRun:
    job_id: str
    status: JobStatus = JobStatus.PENDING
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    error_message: str | None = None
    scheduled_for: datetime | None = None
    id: str = field(default_factory=_uuid)
