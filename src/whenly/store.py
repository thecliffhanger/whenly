"""SQLite + in-memory store for whenly."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import Any

from .models import Job, JobRun, JobStatus, MissedPolicy, ScheduleType

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    func_path TEXT NOT NULL,
    schedule_type TEXT NOT NULL,
    interval_seconds INTEGER,
    cron_expr TEXT,
    scheduled_for TEXT,
    missed_policy TEXT DEFAULT 'run_once',
    max_concurrent INTEGER DEFAULT 1,
    timeout_seconds INTEGER,
    enabled INTEGER DEFAULT 1,
    next_run_at TEXT,
    last_run_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS job_runs (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id),
    status TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    duration_seconds REAL,
    error_message TEXT,
    scheduled_for TEXT
);
"""


def _dt(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


def _dts(d: datetime | None) -> str | None:
    return d.isoformat() if d else None


class Store:
    """Persistent store backend (SQLite or :memory:)."""

    def __init__(self, db_path: str = "whenly.db") -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None
        self._connect()
        self._conn.execute("PRAGMA journal_mode=WAL")  # type: ignore[union-attr]
        self._conn.executescript(_SCHEMA)  # type: ignore[union-attr]

    # -- connection management --

    def _connect(self) -> None:
        detect = sqlite3.PARSE_DECLTYPES
        self._conn = sqlite3.connect(self._db_path, detect_types=detect, check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.row_factory = sqlite3.Row  # type: ignore[union-attr]

    @property
    def conn(self) -> sqlite3.Connection:
        assert self._conn is not None
        return self._conn

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # -- jobs CRUD --

    def save_job(self, job: Job) -> Job:
        row = self.conn.execute(
            """INSERT INTO jobs (id, name, func_path, schedule_type, interval_seconds,
               cron_expr, scheduled_for, missed_policy, max_concurrent, timeout_seconds,
               enabled, next_run_at, last_run_at, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(name) DO UPDATE SET
                 func_path=excluded.func_path, schedule_type=excluded.schedule_type,
                 interval_seconds=excluded.interval_seconds, cron_expr=excluded.cron_expr,
                 scheduled_for=excluded.scheduled_for, missed_policy=excluded.missed_policy,
                 max_concurrent=excluded.max_concurrent, timeout_seconds=excluded.timeout_seconds,
                 enabled=excluded.enabled, next_run_at=excluded.next_run_at,
                 last_run_at=excluded.last_run_at, updated_at=excluded.updated_at""",
            (
                job.id, job.name, job.func_path, job.schedule_type.value,
                job.interval_seconds, job.cron_expr, _dts(job.scheduled_for),
                job.missed_policy.value, job.max_concurrent, job.timeout_seconds,
                int(job.enabled), _dts(job.next_run_at), _dts(job.last_run_at),
                job.created_at.isoformat(), job.updated_at.isoformat(),
            ),
        )
        self.conn.commit()
        result = self.get_job(job.name)
        return result if result else job

    def get_job(self, name: str) -> Job | None:
        row = self.conn.execute("SELECT * FROM jobs WHERE name=?", (name,)).fetchone()
        return self._row_to_job(row) if row else None

    def get_job_by_id(self, job_id: str) -> Job | None:
        row = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._row_to_job(row) if row else None

    def list_jobs(self) -> list[Job]:
        rows = self.conn.execute("SELECT * FROM jobs ORDER BY created_at").fetchall()
        return [self._row_to_job(r) for r in rows]

    def delete_job(self, name: str) -> bool:
        # Delete associated runs first to satisfy FK constraint
        job = self.get_job(name)
        if job:
            self.conn.execute("DELETE FROM job_runs WHERE job_id=?", (job.id,))
        cur = self.conn.execute("DELETE FROM jobs WHERE name=?", (name,))
        self.conn.commit()
        return cur.rowcount > 0

    def update_next_run(self, job_id: str, next_run: datetime) -> None:
        self.conn.execute(
            "UPDATE jobs SET next_run_at=?, updated_at=? WHERE id=?",
            (next_run.isoformat(), datetime.utcnow().isoformat(), job_id),
        )
        self.conn.commit()

    def update_last_run(self, job_id: str, last_run: datetime) -> None:
        self.conn.execute(
            "UPDATE jobs SET last_run_at=?, updated_at=? WHERE id=?",
            (last_run.isoformat(), datetime.utcnow().isoformat(), job_id),
        )
        self.conn.commit()

    def set_enabled(self, job_id: str, enabled: bool) -> None:
        self.conn.execute(
            "UPDATE jobs SET enabled=?, updated_at=? WHERE id=?",
            (int(enabled), datetime.utcnow().isoformat(), job_id),
        )
        self.conn.commit()

    # -- due jobs + missed runs --

    def get_due_jobs(self, now: datetime) -> list[Job]:
        rows = self.conn.execute(
            """SELECT * FROM jobs
               WHERE enabled=1 AND next_run_at IS NOT NULL AND next_run_at <= ?
               ORDER BY next_run_at""",
            (now.isoformat(),),
        ).fetchall()
        return [self._row_to_job(r) for r in rows]

    def get_running_count(self, job_id: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) FROM job_runs WHERE job_id=? AND status='running'", (job_id,)
        ).fetchone()
        return row[0]

    def get_missed_count(self, job_id: str, now: datetime) -> int:
        """Count how many scheduled runs were missed between last_run_at and now."""
        job = self.get_job_by_id(job_id)
        if not job:
            return 0
        if job.missed_policy == MissedPolicy.SKIP:
            return 0
        # For simplicity: interval jobs can calculate count; cron/once just 0 or 1
        if job.schedule_type == ScheduleType.INTERVAL and job.interval_seconds and job.last_run_at:
            elapsed = (now - job.last_run_at).total_seconds()
            count = int(elapsed // job.interval_seconds)
            return max(0, count - 1) if job.next_run_at else max(0, count)
        return 0

    # -- job runs --

    def save_run(self, run: JobRun) -> JobRun:
        self.conn.execute(
            """INSERT INTO job_runs (id, job_id, status, started_at, finished_at,
               duration_seconds, error_message, scheduled_for)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                run.id, run.job_id, run.status.value,
                _dts(run.started_at), _dts(run.finished_at),
                run.duration_seconds, run.error_message, _dts(run.scheduled_for),
            ),
        )
        self.conn.commit()
        return run

    def update_run(self, run: JobRun) -> None:
        self.conn.execute(
            """UPDATE job_runs SET status=?, started_at=?, finished_at=?,
               duration_seconds=?, error_message=? WHERE id=?""",
            (run.status.value, _dts(run.started_at), _dts(run.finished_at),
             run.duration_seconds, run.error_message, run.id),
        )
        self.conn.commit()

    def get_runs(self, job_name: str, limit: int = 20) -> list[JobRun]:
        rows = self.conn.execute(
            """SELECT jr.* FROM job_runs jr
               JOIN jobs j ON jr.job_id = j.id WHERE j.name=?
               ORDER BY jr.started_at DESC LIMIT ?""",
            (job_name, limit),
        ).fetchall()
        return [self._row_to_run(r) for r in rows]

    # -- internals --

    def _row_to_job(self, row: sqlite3.Row) -> Job:  # type: ignore[type-arg]
        return Job(
            id=row["id"],
            name=row["name"],
            func_path=row["func_path"],
            schedule_type=ScheduleType(row["schedule_type"]),
            interval_seconds=row["interval_seconds"],
            cron_expr=row["cron_expr"],
            scheduled_for=_dt(row["scheduled_for"]),
            missed_policy=MissedPolicy(row["missed_policy"]),
            max_concurrent=row["max_concurrent"],
            timeout_seconds=row["timeout_seconds"],
            enabled=bool(row["enabled"]),
            next_run_at=_dt(row["next_run_at"]),
            last_run_at=_dt(row["last_run_at"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def _row_to_run(self, row: sqlite3.Row) -> JobRun:  # type: ignore[type-arg]
        return JobRun(
            id=row["id"],
            job_id=row["job_id"],
            status=JobStatus(row["status"]),
            started_at=_dt(row["started_at"]),
            finished_at=_dt(row["finished_at"]),
            duration_seconds=row["duration_seconds"],
            error_message=row["error_message"],
            scheduled_for=_dt(row["scheduled_for"]),
        )
