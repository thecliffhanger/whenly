"""Job execution engine."""

from __future__ import annotations

import logging
import threading
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable

from .models import Job, JobRun, JobStatus
from .store import Store

logger = logging.getLogger(__name__)


class JobRunner:
    """Executes jobs in a thread pool."""

    def __init__(self, store: Store, max_workers: int = 10) -> None:
        self._store = store
        self._max_workers = max_workers
        self._executor: ThreadPoolExecutor | None = None
        self._futures: dict[str, Future[None]] = {}
        self._lock = threading.Lock()

    def _ensure_executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        return self._executor

    def submit(self, job: Job, scheduled_for: datetime | None = None) -> bool:
        """Submit a job for execution. Returns True if submitted."""
        if not job.enabled:
            return False
        running = self._store.get_running_count(job.id)
        if running >= job.max_concurrent:
            logger.debug("Job %s at max concurrent (%d), skipping", job.name, job.max_concurrent)
            return False

        run = JobRun(
            job_id=job.id,
            status=JobStatus.RUNNING,
            started_at=datetime.utcnow(),
            scheduled_for=scheduled_for,
        )
        self._store.save_run(run)

        executor = self._ensure_executor()
        with self._lock:
            self._futures[run.id] = executor.submit(self._execute, job, run)
        return True

    def _execute(self, job: Job, run: JobRun) -> None:
        func: Callable[..., Any] = job.func
        if func is None:
            func = _resolve_func(job.func_path)
            if func is None:
                run.status = JobStatus.FAILED
                run.error_message = f"Cannot resolve function: {job.func_path}"
                self._store.update_run(run)
                with self._lock:
                    self._futures.pop(run.id, None)
                return

        try:
            args = getattr(job, '_args', ()) or ()
            kwargs = getattr(job, '_kwargs', {}) or {}
            result = func(*args, **kwargs)
            # Check if result is a future we should wait on
            if isinstance(result, Future):
                result.result(timeout=job.timeout_seconds)
        except Exception as exc:
            if isinstance(exc, TimeoutError):
                run.status = JobStatus.TIMEOUT
            else:
                run.status = JobStatus.FAILED
            run.error_message = traceback.format_exc()
            logger.error("Job %s failed: %s", job.name, exc)
        finally:
            run.finished_at = datetime.utcnow()
            run.duration_seconds = (run.finished_at - (run.started_at or run.finished_at)).total_seconds()
            if run.status == JobStatus.RUNNING:
                run.status = JobStatus.SUCCESS
            self._store.update_run(run)
            self._store.update_last_run(job.id, run.finished_at)

        with self._lock:
            self._futures.pop(run.id, None)

    def shutdown(self, wait: bool = True) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=wait)
            self._executor = None
        with self._lock:
            self._futures.clear()


def _resolve_func(func_path: str) -> Callable[..., Any] | None:
    """Import a function from its dotted path (e.g. 'mymodule.jobs.sync_data')."""
    try:
        parts = func_path.rsplit(".", 1)
        if len(parts) == 2:
            mod_path, func_name = parts
            import importlib
            mod = importlib.import_module(mod_path)
            return getattr(mod, func_name, None)
    except Exception:
        logger.debug("Cannot resolve %s", func_path, exc_info=True)
    return None
