"""Main Scheduler class for whenly."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable

from croniter import croniter

from .models import Job, JobRun, MissedPolicy, ScheduleType
from .runner import JobRunner
from .store import Store

logger = logging.getLogger(__name__)


class Scheduler:
    """Lightweight persistent job scheduler."""

    def __init__(
        self,
        db_path: str = "whenly.db",
        tick_interval: float = 1.0,
        max_workers: int = 10,
    ) -> None:
        self._store = Store(db_path)
        self._runner = JobRunner(self._store, max_workers=max_workers)
        self._tick_interval = tick_interval
        self._thread: threading.Thread | None = None
        self._running = False
        self._lock = threading.Lock()
        self._funcs: dict[str, Callable[..., Any]] = {}

    # -- decorators --

    def every(
        self,
        amount: int = 1,
        unit: str = "seconds",
        *,
        name: str | None = None,
        missed: str | MissedPolicy = MissedPolicy.RUN_ONCE,
        timeout: int | None = None,
        max_concurrent: int = 1,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator to register an interval job.

        Usage:
            @s.every(5, "minutes")
            def my_job(): ...
            @s.every(10, minutes=True)
            def my_job(): ...
        """
        seconds = _parse_interval(amount, unit)
        policy = MissedPolicy(missed) if isinstance(missed, str) else missed

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            job_name = name or fn.__name__
            with self._lock:
                self._funcs[job_name] = fn
            job = Job(
                name=job_name,
                func_path=_func_path(fn),
                schedule_type=ScheduleType.INTERVAL,
                interval_seconds=seconds,
                missed_policy=policy,
                timeout_seconds=timeout,
                max_concurrent=max_concurrent,
                next_run_at=datetime.utcnow(),
                func=fn,
            )
            self._store.save_job(job)
            return fn
        return decorator

    def cron(
        self,
        expr: str,
        *,
        name: str | None = None,
        missed: str | MissedPolicy = MissedPolicy.SKIP,
        timeout: int | None = None,
        max_concurrent: int = 1,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator to register a cron job."""
        policy = MissedPolicy(missed) if isinstance(missed, str) else missed

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            job_name = name or fn.__name__
            with self._lock:
                self._funcs[job_name] = fn
            next_run = croniter(expr, datetime.utcnow()).get_next(datetime)
            job = Job(
                name=job_name,
                func_path=_func_path(fn),
                schedule_type=ScheduleType.CRON,
                cron_expr=expr,
                missed_policy=policy,
                timeout_seconds=timeout,
                max_concurrent=max_concurrent,
                next_run_at=next_run,
                func=fn,
            )
            self._store.save_job(job)
            return fn
        return decorator

    # -- programmatic API --

    def add(
        self,
        func: Callable[..., Any],
        *,
        every: str | None = None,
        cron: str | None = None,
        name: str | None = None,
        missed: str | MissedPolicy = MissedPolicy.RUN_ONCE,
        timeout: int | None = None,
        max_concurrent: int = 1,
    ) -> Job:
        """Add a job programmatically."""
        policy = MissedPolicy(missed) if isinstance(missed, str) else missed
        job_name = name or func.__name__
        with self._lock:
            self._funcs[job_name] = func

        if cron:
            next_run = croniter(cron, datetime.utcnow()).get_next(datetime)
            job = Job(
                name=job_name, func_path=_func_path(func),
                schedule_type=ScheduleType.CRON, cron_expr=cron,
                missed_policy=policy, timeout_seconds=timeout,
                max_concurrent=max_concurrent, next_run_at=next_run, func=func,
            )
        elif every:
            seconds = _parse_interval_str(every)
            job = Job(
                name=job_name, func_path=_func_path(func),
                schedule_type=ScheduleType.INTERVAL, interval_seconds=seconds,
                missed_policy=policy, timeout_seconds=timeout,
                max_concurrent=max_concurrent, next_run_at=datetime.utcnow(), func=func,
            )
        else:
            raise ValueError("Must specify either 'every' or 'cron'")

        return self._store.save_job(job)

    def later(
        self,
        amount: int = 1,
        unit: str = "seconds",
        func: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        args: tuple = (),
        kwargs: dict | None = None,
    ) -> Job | Callable[[Callable[..., Any]], Job]:
        """Schedule a one-off job to run after a delay.

        Can be used as a method or decorator:
            s.later(30, "seconds", send_email)
            @s.later(5, "minutes")
            def cleanup(): ...
        """
        seconds = _parse_interval(amount, unit)
        _kwargs = kwargs or {}

        if func is not None:
            return self._create_oneoff(func, seconds, name or func.__name__, args, _kwargs)

        def decorator(fn: Callable[..., Any]) -> Job:
            return self._create_oneoff(fn, seconds, name or fn.__name__, args, _kwargs)

        return decorator  # type: ignore[return-value]

    def _create_oneoff(
        self, func: Callable[..., Any], delay_seconds: int, name: str,
        args: tuple = (), kwargs: dict | None = None,
    ) -> Job:
        job_name = name
        _kwargs = kwargs or {}
        with self._lock:
            self._funcs[job_name] = func
        scheduled = datetime.utcnow() + timedelta(seconds=delay_seconds)
        job = Job(
            name=job_name, func_path=_func_path(func),
            schedule_type=ScheduleType.ONCE, scheduled_for=scheduled,
            next_run_at=scheduled, func=func,
        )
        job._args = args  # type: ignore[attr-defined]
        job._kwargs = _kwargs  # type: ignore[attr-defined]
        return self._store.save_job(job)

    def run_now(self, name: str) -> bool:
        """Manually trigger a job by name."""
        job = self._store.get_job(name)
        if not job:
            return False
        # Attach runtime func if available
        with self._lock:
            job.func = self._funcs.get(job.name)
        self._runner.submit(job)
        return True

    def disable(self, name: str) -> bool:
        job = self._store.get_job(name)
        if not job:
            return False
        self._store.set_enabled(job.id, False)
        return True

    def enable(self, name: str) -> bool:
        job = self._store.get_job(name)
        if not job:
            return False
        self._store.set_enabled(job.id, True)
        return True

    # -- scheduler loop --

    def start(self) -> None:
        """Start the scheduler in a background thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()
            logger.info("Scheduler started (background)")

    def stop(self) -> None:
        """Stop the scheduler."""
        with self._lock:
            self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        self._runner.shutdown(wait=True)
        logger.info("Scheduler stopped")

    def run(self) -> None:
        """Run the scheduler blocking (foreground)."""
        with self._lock:
            if self._running:
                return
            self._running = True
        try:
            self._loop()
        finally:
            self._running = False
            self._runner.shutdown(wait=True)

    def _loop(self) -> None:
        while self._running:
            self._tick()
            time.sleep(self._tick_interval)

    def _tick(self) -> None:
        now = datetime.utcnow()
        due_jobs = self._store.get_due_jobs(now)

        for job in due_jobs:
            # Attach runtime func if available
            with self._lock:
                job.func = self._funcs.get(job.name)
            submitted = self._runner.submit(job, scheduled_for=job.next_run_at)

            if submitted:
                # Handle missed runs and schedule next
                if job.schedule_type == ScheduleType.ONCE:
                    # One-off: clear next_run to prevent re-scheduling
                    self._store.update_next_run(job.id, now + timedelta(days=365 * 100))
                    self._store.set_enabled(job.id, False)
                    continue

                # Compute next run FIRST to avoid duplicates
                next_run = self._compute_next(job, now)
                self._store.update_next_run(job.id, next_run)

                # Handle missed runs
                if job.missed_policy == MissedPolicy.RUN_ALL:
                    missed = self._store.get_missed_count(job.id, now)
                    for i in range(missed - 1):
                        missed_job = Job(
                            **{k: getattr(job, k) for k in job.__dataclass_fields__ if k != "func"},
                            func=job.func,
                        )
                        self._runner.submit(missed_job)

    def _compute_next(self, job: Job, after: datetime) -> datetime:
        if job.schedule_type == ScheduleType.CRON and job.cron_expr:
            return croniter(job.cron_expr, after).get_next(datetime)
        elif job.schedule_type == ScheduleType.INTERVAL and job.interval_seconds:
            return after + timedelta(seconds=job.interval_seconds)
        return after + timedelta(seconds=60)

    # -- accessors --

    @property
    def store(self) -> Store:
        return self._store

    @property
    def jobs(self) -> list[Job]:
        return self._store.list_jobs()

    def get_history(self, name: str, limit: int = 20) -> list[JobRun]:
        return self._store.get_runs(name, limit)


# -- helpers --

def _parse_interval(amount: int, unit: str) -> int:
    """Parse interval amount + unit to seconds."""
    unit = unit.lower().rstrip("s")
    multipliers = {
        "second": 1, "minute": 60, "hour": 3600, "day": 86400,
    }
    if unit not in multipliers:
        raise ValueError(f"Unknown unit: {unit}. Use seconds/minutes/hours/days")
    return amount * multipliers[unit]


def _parse_interval_str(s: str) -> int | float:
    """Parse human interval like '5m', '2h', '30s', '1d'."""
    s = s.strip().lower()
    units = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}
    for suffix, mult in units.items():
        if s.endswith(suffix):
            try:
                return int(s[:-len(suffix)]) * mult
            except ValueError:
                raise ValueError(f"Invalid interval: {s}")
    raise ValueError(f"Invalid interval: {s}. Use format like '5m', '2h', '30s', '1d'")


def _func_path(fn: Callable[..., Any]) -> str:
    return f"{fn.__module__}.{fn.__qualname__}"
