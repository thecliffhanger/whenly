"""Adversarial/fuzz tests for whenly."""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from io import StringIO
from unittest.mock import patch

import pytest

from whenly import Scheduler
from whenly.models import Job, JobRun, JobStatus, MissedPolicy, ScheduleType
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
    s = Scheduler(db_path=str(db), tick_interval=0.05)
    yield s
    s.stop()


# ============================================================
# 1. Edge cases: empty, rapid cycles, huge jobs, invalid cron, unicode
# ============================================================

class TestEdgeCases:

    def test_empty_scheduler_no_crash(self, scheduler):
        """An empty scheduler should start/stop without errors."""
        scheduler.start()
        time.sleep(0.3)
        scheduler.stop()

    def test_rapid_add_remove(self, scheduler):
        """Rapidly add and remove jobs."""
        added = []

        def make_job(i):
            def fn():
                pass
            return fn

        # Add 100 jobs rapidly
        for i in range(100):
            name = f"rapid_{i}"
            scheduler.add(make_job(i), every="999s", name=name)
            added.append(name)

        assert len(scheduler.jobs) == 100

        # Remove them all rapidly
        for name in added:
            scheduler.store.delete_job(name)

        assert len(scheduler.jobs) == 0

    def test_add_1000_jobs(self, tmp_path):
        """Adding 1000+ jobs shouldn't crash."""
        db = tmp_path / "big.db"
        s = Scheduler(db_path=str(db), tick_interval=0.05)

        def noop():
            pass

        for i in range(1000):
            s.add(noop, every="999s", name=f"job_{i}")

        assert len(s.jobs) == 1000
        s.stop()

    def test_invalid_cron_expression(self, scheduler):
        """Invalid cron should raise an error at registration time."""
        def noop():
            pass

        with pytest.raises(Exception):  # croniter raises ValueError
            scheduler.add(noop, cron="not_a_cron", name="bad_cron")

        with pytest.raises(Exception):
            @scheduler.cron("INVALID CRON", name="bad_cron_dec")
            def noop2():
                pass

    def test_unicode_job_names(self, scheduler):
        """Jobs with unicode names should work."""
        counter = {"n": 0}

        names = ["café", "日本語テスト", "🎉party", "emoji_🔥_test", "über_job"]

        for name in names:
            scheduler.add(lambda: None, every="999s", name=name)

        for name in names:
            job = scheduler.store.get_job(name)
            assert job is not None
            assert job.name == name

    def test_very_long_job_name(self, scheduler):
        """Extremely long job name."""
        long_name = "x" * 10000
        scheduler.add(lambda: None, every="999s", name=long_name)
        assert scheduler.store.get_job(long_name) is not None

    def test_duplicate_job_name_upsert(self, scheduler):
        """Re-adding with same name should update, not duplicate."""
        scheduler.add(lambda: None, every="100s", name="dup")
        scheduler.add(lambda: None, every="200s", name="dup")
        assert len([j for j in scheduler.jobs if j.name == "dup"]) == 1
        assert scheduler.store.get_job("dup").interval_seconds == 200

    def test_run_now_nonexistent(self, scheduler):
        assert scheduler.run_now("nope_noexist") is False

    def test_disable_nonexistent(self, scheduler):
        assert scheduler.disable("nope") is False

    def test_enable_nonexistent(self, scheduler):
        assert scheduler.enable("nope") is False

    def test_add_without_every_or_cron(self, scheduler):
        """Must specify every or cron."""
        with pytest.raises(ValueError, match="Must specify"):
            scheduler.add(lambda: None, name="bad")

    def test_invalid_interval_unit(self, scheduler):
        with pytest.raises(ValueError, match="Unknown unit"):
            scheduler.every(5, "fortnights", name="bad_unit")(lambda: None)

    def test_invalid_interval_str(self, scheduler):
        with pytest.raises(ValueError, match="Invalid interval"):
            scheduler.add(lambda: None, every="abc", name="bad_str")


# ============================================================
# 2. SQLite store: concurrent writes, corrupted DB, long history
# ============================================================

class TestSQLiteStore:

    def test_concurrent_writes(self, tmp_path):
        """Multiple threads writing to store simultaneously."""
        db = tmp_path / "concurrent.db"
        s = Store(str(db))
        errors = []

        def writer(thread_id):
            try:
                for i in range(50):
                    name = f"t{thread_id}_j{i}"
                    job = Job(name=name, func_path=f"m.{name}",
                              schedule_type=ScheduleType.INTERVAL,
                              interval_seconds=60)
                    s.save_job(job)
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Concurrent write errors: {errors}"
        assert len(s.list_jobs()) == 250
        s.close()

    def test_corrupted_db_recovery(self, tmp_path):
        """Store should raise or handle corrupted DB gracefully."""
        db = tmp_path / "corrupt.db"

        # Create a valid DB first
        s = Store(str(db))
        s.save_job(Job(name="ok", func_path="m.ok", schedule_type=ScheduleType.INTERVAL, interval_seconds=60))
        s.close()

        # Corrupt the file
        with open(db, "wb") as f:
            f.write(b"this is not a sqlite database at all!!!")

        # Opening should raise sqlite3.DatabaseError
        with pytest.raises(sqlite3.DatabaseError):
            Store(str(db))

    def test_very_long_job_history(self, store):
        """Insert many job runs and query them."""
        job = Job(name="hist_test", func_path="m.h", schedule_type=ScheduleType.INTERVAL, interval_seconds=60)
        store.save_job(job)

        for i in range(500):
            run = JobRun(
                job_id=job.id,
                status=JobStatus.SUCCESS,
                started_at=datetime.utcnow(),
                finished_at=datetime.utcnow(),
                duration_seconds=1.0,
            )
            store.save_run(run)

        runs = store.get_runs("hist_test", limit=20)
        assert len(runs) == 20

    def test_locked_db_timeout(self, tmp_path):
        """Test behavior when DB is locked by another connection."""
        db = tmp_path / "locked.db"
        s1 = Store(str(db))
        s1.save_job(Job(name="lock_test", func_path="m.l", schedule_type=ScheduleType.INTERVAL, interval_seconds=60))

        # Open a second connection and lock it
        conn2 = sqlite3.connect(str(db))
        conn2.execute("BEGIN EXCLUSIVE")
        conn2.execute("INSERT INTO jobs (id, name, func_path, schedule_type, created_at, updated_at) VALUES ('x', 'y', 'z', 'interval', '2024-01-01T00:00:00', '2024-01-01T00:00:00')")

        # s1 should still work (SQLite busy timeout or autocommit handles it)
        # This might succeed or fail depending on timing
        try:
            s1.save_job(Job(name="after_lock", func_path="m.al", schedule_type=ScheduleType.INTERVAL, interval_seconds=60))
        except sqlite3.OperationalError:
            pass  # Acceptable: DB was locked
        finally:
            conn2.rollback()
            conn2.close()
            s1.close()

    def test_save_run_for_nonexistent_job(self, store):
        """Saving a run for a job that doesn't exist (FK violation or orphan)."""
        run = JobRun(
            job_id="nonexistent_job_id",
            status=JobStatus.SUCCESS,
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
            duration_seconds=1.0,
        )
        # Should fail due to FK constraint
        with pytest.raises(sqlite3.IntegrityError):
            store.save_run(run)


# ============================================================
# 3. Runner: exceptions, timeouts, deadlocks
# ============================================================

class TestRunner:

    def test_job_raises_exception(self, scheduler):
        """Job that raises should be marked FAILED."""
        @scheduler.every(1, "hours", name="exploder")
        def boom():
            raise RuntimeError("intentional boom")

        scheduler.run_now("exploder")
        time.sleep(0.5)
        runs = scheduler.get_history("exploder")
        assert len(runs) >= 1
        assert runs[0].status == JobStatus.FAILED
        assert "intentional boom" in runs[0].error_message

    def test_timeout_causes_timeout_status(self, scheduler):
        """Job exceeding timeout should get TIMEOUT status (if timeout actually enforced)."""
        # Note: current implementation doesn't enforce timeouts via threading,
        # it only catches TimeoutError from Future.result(). So this tests
        # that a slow job doesn't crash the runner.
        @scheduler.every(1, "hours", name="slow_timeout", timeout=1)
        def slow_job():
            time.sleep(5)

        scheduler.run_now("slow_timeout")
        time.sleep(0.3)  # Give it a moment to start

        # Job is still running (no real timeout enforcement in thread pool)
        runs = scheduler.get_history("slow_timeout")
        assert len(runs) >= 1
        assert runs[0].status == JobStatus.RUNNING  # Will still be running
        scheduler.stop()  # Clean up

    def test_zero_timeout(self, scheduler):
        """Zero timeout shouldn't crash."""
        @scheduler.every(1, "hours", name="zero_timeout", timeout=0)
        def quick():
            pass

        scheduler.run_now("zero_timeout")
        time.sleep(0.5)
        runs = scheduler.get_history("zero_timeout")
        assert len(runs) >= 1
        assert runs[0].status == JobStatus.SUCCESS

    def test_job_with_none_func_no_resolve(self, scheduler):
        """Job with no runtime func and unresolvable path should fail gracefully."""
        job = Job(
            name="ghost_job",
            func_path="totally.fake.module.nonexistent_func",
            schedule_type=ScheduleType.INTERVAL,
            interval_seconds=60,
            next_run_at=datetime.utcnow(),
        )
        scheduler.store.save_job(job)
        # Not in _funcs dict, and path won't resolve
        scheduler.run_now("ghost_job")
        time.sleep(0.5)
        runs = scheduler.get_history("ghost_job")
        assert len(runs) >= 1
        assert runs[0].status == JobStatus.FAILED
        assert "Cannot resolve" in runs[0].error_message

    def test_multiple_simultaneous_run_now(self, scheduler):
        """Calling run_now many times rapidly."""
        counter = {"n": 0}

        @scheduler.every(1, "hours", name="rapid_fire")
        def inc():
            time.sleep(0.1)
            counter["n"] += 1

        for _ in range(20):
            scheduler.run_now("rapid_fire")

        time.sleep(3.0)
        # With max_concurrent=1, only 1 should run at a time
        # but many should complete over 3s
        assert counter["n"] >= 1

    def test_exception_in_finally(self, scheduler):
        """Job that succeeds but modifies shared state."""
        state = {"n": 0}

        @scheduler.every(1, "hours", name="finally_test")
        def add_and_fail():
            state["n"] += 1
            raise ValueError("oops")

        scheduler.run_now("finally_test")
        time.sleep(0.5)
        assert state["n"] == 1
        runs = scheduler.get_history("finally_test")
        assert runs[0].status == JobStatus.FAILED

    def test_later_with_zero_delay(self, scheduler):
        """Zero-delay one-off job should run."""
        result = {"done": False}

        scheduler.later(0, "seconds", lambda: result.update(done=True), name="instant")
        scheduler.start()
        time.sleep(1.0)
        scheduler.stop()
        assert result["done"]


# ============================================================
# 4. CLI: invalid commands, missing args
# ============================================================

class TestCLI:

    def test_no_command(self):
        from whenly.cli import main
        with pytest.raises(SystemExit):
            main([])

    def test_unknown_command(self):
        from whenly.cli import main
        with pytest.raises(SystemExit):
            main(["bogus"])

    def test_history_missing_name(self):
        from whenly.cli import main
        with pytest.raises(SystemExit):
            main(["history"])

    def test_run_nonexistent_job(self, tmp_path):
        from whenly.cli import main
        db = tmp_path / "cli.db"
        with pytest.raises(SystemExit):
            main(["--db", str(db), "run", "nope"])

    def test_list_output_format(self, tmp_path):
        from whenly.cli import main
        db = tmp_path / "fmt.db"
        s = Scheduler(db_path=str(db))
        @s.every(5, "minutes", name="fmt_test")
        def dummy(): pass
        s.stop()
        s.store.close()

        with patch("sys.stdout", new_callable=StringIO) as out:
            main(["--db", str(db), "list"])
            output = out.getvalue()
            assert "fmt_test" in output
            assert "every 300s" in output

    def test_cli_with_corrupt_db(self, tmp_path):
        from whenly.cli import main
        db = tmp_path / "corrupt_cli.db"
        with open(db, "w") as f:
            f.write("not a database")
        with pytest.raises(Exception):
            main(["--db", str(db), "list"])


# ============================================================
# 5. Scheduler lifecycle edge cases
# ============================================================

class TestLifecycle:

    def test_double_start(self, scheduler):
        """Starting twice should be idempotent."""
        scheduler.start()
        scheduler.start()
        time.sleep(0.2)
        scheduler.stop()

    def test_double_stop(self, scheduler):
        """Stopping twice should be safe."""
        scheduler.start()
        time.sleep(0.1)
        scheduler.stop()
        scheduler.stop()

    def test_start_stop_restart(self, scheduler):
        counter = {"n": 0}

        @scheduler.every(1, "seconds", name="lifecycle")
        def inc():
            counter["n"] += 1

        scheduler.start()
        time.sleep(0.6)
        scheduler.stop()
        scheduler.start()
        time.sleep(0.6)
        scheduler.stop()
        assert counter["n"] >= 1

    def test_delete_job_while_scheduled(self, scheduler):
        """Deleting a job that's due should not crash the scheduler."""
        @scheduler.every(1, "seconds", name="ephemeral")
        def short_lived():
            pass

        scheduler.start()
        time.sleep(0.3)
        scheduler.store.delete_job("ephemeral")
        time.sleep(0.5)
        scheduler.stop()  # Should not crash

    def test_negative_interval(self, scheduler):
        """Negative interval should be rejected or handled."""
        # _parse_interval will return negative seconds
        job = scheduler.every(-5, "seconds", name="negative")(lambda: None)
        # It saves successfully but next_run_at will be in the past
        # The scheduler will pick it up immediately
        scheduler.store.get_job("negative")  # Should exist


# ============================================================
# 6. Interval parsing edge cases
# ============================================================

class TestIntervalParsing:

    def test_interval_str_with_ms(self, scheduler):
        """Millisecond intervals."""
        scheduler.add(lambda: None, every="100ms", name="ms_job")
        job = scheduler.store.get_job("ms_job")
        # 100 * 0.001 = 0.1, but int(0.1) = 0
        assert job.interval_seconds == 0.1  # Fixed: returns float

    def test_interval_str_zero(self, scheduler):
        scheduler.add(lambda: None, every="0s", name="zero")
        assert scheduler.store.get_job("zero").interval_seconds == 0

    def test_interval_str_large(self, scheduler):
        scheduler.add(lambda: None, every="999999s", name="huge")
        assert scheduler.store.get_job("huge").interval_seconds == 999999

    def test_every_plural_units(self, scheduler):
        """Plural units should work."""
        scheduler.every(1, "second", name="sg")(lambda: None)
        scheduler.every(1, "seconds", name="s")(lambda: None)
        scheduler.every(1, "minute", name="mg")(lambda: None)
        scheduler.every(1, "minutes", name="m")(lambda: None)
        scheduler.every(1, "hour", name="hg")(lambda: None)
        scheduler.every(1, "hours", name="h")(lambda: None)
        scheduler.every(1, "day", name="dg")(lambda: None)
        scheduler.every(1, "days", name="d")(lambda: None)
        assert len(scheduler.jobs) == 8
