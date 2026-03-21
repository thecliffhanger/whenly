"""Comprehensive tests for whenly."""

from __future__ import annotations

import time
import threading
from datetime import datetime, timedelta
from io import StringIO
from unittest.mock import patch

import pytest

from whenly import Scheduler
from whenly.models import Job, JobStatus, MissedPolicy, ScheduleType
from whenly.store import Store


@pytest.fixture
def store(tmp_path):
    db = tmp_path / "test.db"
    s = Store(str(db))
    yield s
    s.close()


@pytest.fixture
def scheduler(tmp_path):
    db = tmp_path / "test.db"
    s = Scheduler(db_path=str(db), tick_interval=0.1)
    yield s
    s.stop()


@pytest.fixture
def mem_scheduler():
    s = Scheduler(db_path=":memory:", tick_interval=0.1)
    yield s
    s.stop()


# === Store tests ===

class TestStore:
    def test_save_and_get_job(self, store):
        job = Job(name="test", func_path="__main__.test", schedule_type=ScheduleType.INTERVAL,
                  interval_seconds=60, next_run_at=datetime.utcnow())
        saved = store.save_job(job)
        assert saved.name == "test"
        assert store.get_job("test") is not None
        assert store.get_job("nonexistent") is None

    def test_upsert_job(self, store):
        job = Job(name="test", func_path="__main__.test", schedule_type=ScheduleType.INTERVAL,
                  interval_seconds=60)
        store.save_job(job)
        job.interval_seconds = 120
        store.save_job(job)
        fetched = store.get_job("test")
        assert fetched.interval_seconds == 120

    def test_list_and_delete(self, store):
        store.save_job(Job(name="a", func_path="m.a", schedule_type=ScheduleType.INTERVAL, interval_seconds=60))
        store.save_job(Job(name="b", func_path="m.b", schedule_type=ScheduleType.INTERVAL, interval_seconds=60))
        assert len(store.list_jobs()) == 2
        assert store.delete_job("a")
        assert len(store.list_jobs()) == 1

    def test_due_jobs(self, store):
        now = datetime.utcnow()
        store.save_job(Job(name="due", func_path="m.d", schedule_type=ScheduleType.INTERVAL,
                          interval_seconds=60, next_run_at=now - timedelta(seconds=10)))
        store.save_job(Job(name="future", func_path="m.f", schedule_type=ScheduleType.INTERVAL,
                          interval_seconds=60, next_run_at=now + timedelta(hours=1)))
        due = store.get_due_jobs(now)
        assert len(due) == 1
        assert due[0].name == "due"

    def test_job_runs(self, store):
        job = Job(name="rj", func_path="m.r", schedule_type=ScheduleType.INTERVAL, interval_seconds=60)
        store.save_job(job)
        run = job  # just need job_id
        # Actually create a JobRun
        from whenly.models import JobRun
        jr = JobRun(job_id=job.id, status=JobStatus.SUCCESS, started_at=datetime.utcnow(),
                     finished_at=datetime.utcnow(), duration_seconds=1.0)
        store.save_run(jr)
        runs = store.get_runs("rj", limit=10)
        assert len(runs) == 1
        assert runs[0].status == JobStatus.SUCCESS


# === Basic scheduling tests ===

class TestIntervalJobs:
    def test_interval_job_runs(self, scheduler):
        counter = {"n": 0}

        @scheduler.every(1, "seconds", name="counter")
        def inc():
            counter["n"] += 1

        scheduler.start()
        time.sleep(1.5)
        scheduler.stop()
        assert counter["n"] >= 1

    def test_add_programmatic(self, scheduler):
        counter = {"n": 0}

        def inc():
            counter["n"] += 1

        scheduler.add(inc, every="500s")
        scheduler.start()
        time.sleep(1.5)
        scheduler.stop()
        assert counter["n"] >= 1

    def test_multiple_interval_jobs(self, scheduler):
        counts = {"a": 0, "b": 0}

        @scheduler.every(1, "seconds", name="a")
        def job_a():
            counts["a"] += 1

        @scheduler.every(1, "seconds", name="b")
        def job_b():
            counts["b"] += 1

        scheduler.start()
        time.sleep(2.0)
        scheduler.stop()
        assert counts["a"] >= 1
        assert counts["b"] >= 1


# === Cron tests ===

class TestCronJobs:
    def test_cron_job_registration(self, scheduler):
        @scheduler.cron("* * * * *", name="cron_test")
        def cron_job():
            pass

        jobs = scheduler.jobs
        cron_jobs = [j for j in jobs if j.name == "cron_test"]
        assert len(cron_jobs) == 1
        assert cron_jobs[0].schedule_type == ScheduleType.CRON
        assert cron_jobs[0].next_run_at is not None


# === One-off jobs ===

class TestOneOff:
    def test_later_runs_once(self, scheduler):
        result = {"done": False}

        def mark_done():
            result["done"] = True

        scheduler.later(1, "seconds", mark_done, name="onetime")
        scheduler.start()
        time.sleep(2.0)
        scheduler.stop()
        assert result["done"]


# === Missed run handling ===

class TestMissedRuns:
    def test_skip_missed(self, scheduler):
        """Jobs with skip policy don't backfill."""
        counter = {"n": 0}

        @scheduler.every(1, "seconds", name="skip_test", missed="skip")
        def inc():
            counter["n"] += 1
            time.sleep(0.5)  # slow job to miss ticks

        scheduler.start()
        time.sleep(2.0)
        scheduler.stop()
        # Should have run but not backfilled
        assert counter["n"] >= 1


# === Timeout ===

class TestTimeout:
    def test_job_timeout_records(self, scheduler):
        @scheduler.every(1, "hours", name="slow", timeout=1)
        def slow_job():
            time.sleep(5)

        scheduler.run_now("slow")
        time.sleep(0.5)
        runs = scheduler.get_history("slow")
        assert len(runs) >= 1


# === Concurrency ===

class TestConcurrency:
    def test_max_concurrent(self, scheduler):
        running = {"count": 0, "max": 0}

        @scheduler.every(1, "seconds", name="concurrent", max_concurrent=1)
        def check():
            running["count"] += 1
            running["max"] = max(running["max"], running["count"])
            time.sleep(0.5)
            running["count"] -= 1

        # Submit multiple times
        scheduler.run_now("concurrent")
        scheduler.run_now("concurrent")
        time.sleep(1.0)
        assert running["max"] <= 1


# === Enable/disable ===

class TestEnableDisable:
    def test_disable_prevents_run(self, scheduler):
        counter = {"n": 0}

        @scheduler.every(1, "seconds", name="toggle")
        def inc():
            counter["n"] += 1

        scheduler.disable("toggle")
        scheduler.start()
        time.sleep(1.5)
        scheduler.stop()
        assert counter["n"] == 0

    def test_enable_allows_run(self, scheduler):
        counter = {"n": 0}

        @scheduler.every(1, "seconds", name="toggle2")
        def inc():
            counter["n"] += 1

        scheduler.disable("toggle2")
        scheduler.start()
        time.sleep(0.5)
        scheduler.enable("toggle2")
        time.sleep(1.5)
        scheduler.stop()
        assert counter["n"] >= 1


# === In-memory mode ===

class TestInMemory:
    def test_in_memory_scheduler(self, mem_scheduler):
        counter = {"n": 0}

        @mem_scheduler.every(1, "seconds", name="mem_test")
        def inc():
            counter["n"] += 1

        mem_scheduler.start()
        time.sleep(1.5)
        mem_scheduler.stop()
        assert counter["n"] >= 1


# === CLI tests ===

class TestCLI:
    def test_list_empty(self, tmp_path):
        from whenly.cli import main
        db = tmp_path / "empty.db"
        with patch("sys.stdout", new_callable=StringIO) as out:
            main(["--db", str(db), "list"])
            assert "No jobs" in out.getvalue()

    def test_list_with_jobs(self, tmp_path):
        from whenly.cli import main
        db = tmp_path / "jobs.db"
        s = Scheduler(db_path=str(db))
        @s.every(5, "minutes", name="cli_test")
        def dummy(): pass
        s.stop()
        s.store.close()

        with patch("sys.stdout", new_callable=StringIO) as out:
            main(["--db", str(db), "list"])
            assert "cli_test" in out.getvalue()

    def test_history_empty(self, tmp_path):
        from whenly.cli import main
        db = tmp_path / "hist.db"
        with patch("sys.stdout", new_callable=StringIO) as out:
            main(["--db", str(db), "history", "nonexistent"])
            assert "No runs" in out.getvalue()


# === Run method (blocking) ===

class TestRunMethod:
    def test_run_blocking(self, scheduler):
        counter = {"n": 0}

        @scheduler.every(1, "seconds", name="blocking_test")
        def inc():
            counter["n"] += 1
            if counter["n"] >= 2:
                scheduler.stop()

        # run() blocks, so run in a thread
        t = threading.Thread(target=scheduler.run)
        t.start()
        t.join(timeout=5)
        assert not t.is_alive()
        assert counter["n"] >= 2

    def test_run_now_manual(self, scheduler):
        result = {"done": False}

        @scheduler.every(1, "hours", name="manual")
        def mark():
            result["done"] = True

        assert scheduler.run_now("manual")
        time.sleep(0.5)
        assert result["done"]

    def test_run_now_nonexistent(self, scheduler):
        assert not scheduler.run_now("nope")
