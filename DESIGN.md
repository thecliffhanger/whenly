# whenly — Lightweight Persistent Job Scheduler

## What
A Python job scheduler with SQLite persistence, zero external dependencies (except croniter for cron support).
Replaces crontab, lightweight alternative to Celery/APScheduler.

## Core API

```python
from whenly import Scheduler

s = Scheduler()  # SQLite by default, Scheduler(store=":memory:") for in-memory

# Decorator style
@s.every(5, minutes)
def sync_data():
    ...

@s.cron("0 9 * * MON")
def weekly_report():
    ...

s.run()  # Blocking
s.start()  # Background thread
s.stop()

# One-off delayed jobs
s.later(30, minutes, send_email, user_id=42)

# Programmatic style
s.add(func=sync_database, every="5m", name="db-sync", missed="run_once", timeout=300)

# CLI
# whenly list
# whenly history <name>
# whenly run <name>
```

## Features (v1)
- Interval jobs (seconds, minutes, hours, days)
- Cron expression support (via croniter — ONLY external dep)
- One-off delayed jobs
- SQLite persistence (default) + in-memory mode
- Missed-run handling: run_once | run_all | skip
- Max concurrent job control
- Job timeout support
- Success/failure logging
- CLI: list, history, run

## NOT v1
- No distributed mode
- No web dashboard
- No PostgreSQL/Redis backends
- No broker/network layer

## Architecture
- `scheduler.py` — Main Scheduler class, decorator support
- `job.py` — Job definition (interval, cron, one-off)
- `store.py` — SQLite store interface + in-memory fallback
- `runner.py` — Job execution engine (ThreadPoolExecutor)
- `cli.py` — CLI entry point (click or argparse)
- `models.py` — Dataclasses for Job, JobRun

## Store Schema (SQLite)
```sql
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    func_path TEXT NOT NULL,
    schedule_type TEXT NOT NULL,  -- 'interval' | 'cron' | 'once'
    interval_seconds INTEGER,
    cron_expr TEXT,
    scheduled_for TEXT,            -- for one-off jobs
    missed_policy TEXT DEFAULT 'run_once',  -- run_once | run_all | skip
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
    status TEXT NOT NULL,  -- 'success' | 'failed' | 'timeout' | 'running'
    started_at TEXT,
    finished_at TEXT,
    duration_seconds REAL,
    error_message TEXT,
    scheduled_for TEXT  -- when this run was supposed to happen
);
```

## Concurrency
- Thread-based execution using concurrent.futures.ThreadPoolExecutor
- Scheduler main loop checks due jobs every tick_interval (default 1s)
- Jobs run in thread pool, don't block scheduler

## Config
- `Scheduler(db_path="whenly.db")` — default SQLite file in cwd
- `Scheduler(db_path=":memory:")` — in-memory mode
- `Scheduler(tick_interval=1.0)` — scheduler check interval

## Dependencies
- Python 3.10+
- croniter (only external dep, for cron expressions)
- sqlite3 (stdlib)

## Package
- Name: whenly
- Entry: `from whenly import Scheduler`
- CLI: `whenly` command via pyproject.toml scripts entry point
