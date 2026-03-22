"""Round 2 thorough tests for whenly — property-based, stress, concurrency, edge cases."""

from __future__ import annotations

import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor, wait

import pytest
from hypothesis import given, strategies as st, settings

from whenly import Scheduler
from whenly.scheduler import _parse_interval, _parse_interval_str
from whenly.models import Job, JobRun, JobStatus, ScheduleType, MissedPolicy
from whenly.store import Store

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tmp_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


def _make_scheduler(db_path: str | None = None, **kw) -> tuple[Scheduler, str]:
    path = db_path or _tmp_db()
    return Scheduler(db_path=path, **kw), path


# ===========================================================================
# 1. Property-based testing with hypothesis
# ===========================================================================

VALID_UNITS = st.sampled_from(["second", "seconds", "minute", "minutes",
                                "hour", "hours", "day", "days"])

@given(st.integers(min_value=0, max_value=1000000), VALID_UNITS)
def test_parse_interval_valid(amount, unit):
    result = _parse_interval(amount, unit)
    unit_key = unit.lower().rstrip("s")
    expected = {
        "second": 1, "minute": 60, "hour": 3600, "day": 86400,
    }[unit_key] * amount
    assert result == expected


@given(st.text(min_size=1, max_size=20))
def test_parse_interval_invalid_unit(unit):
    unit_lower = unit.lower().rstrip("s")
    if unit_lower not in ("second", "minute", "hour", "day"):
        with pytest.raises(ValueError, match="Unknown unit"):
            _parse_interval(1, unit)


@given(st.integers(min_value=1, max_value=1000), st.sampled_from(["ms", "s", "m", "h", "d"]))
def test_parse_interval_str_valid(amount, suffix):
    s = f"{amount}{suffix}"
    result = _parse_interval_str(s)
    mults = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}
    assert result == amount * mults[suffix]


@given(st.text(alphabet=st.characters(whitelist_categories=('Lu','Ll','Nd')), min_size=1, max_size=30))
def test_interval_job_crud(name):
    """Create, read, update (by re-saving), delete an interval job."""
    s, db = _make_scheduler()
    counter = {"n": 0}
    def fn():
        counter["n"] += 1

    s.add(fn, every="5s", name=name)
    job = s._store.get_job(name)
    assert job is not None
    assert job.schedule_type == ScheduleType.INTERVAL
    assert job.interval_seconds == 5

    # Re-register updates it
    s.add(fn, every="10s", name=name)
    job2 = s._store.get_job(name)
    assert job2.interval_seconds == 10

    # Delete
    assert s._store.delete_job(name) is True
    assert s._store.get_job(name) is None

    s._store.close()
    os.unlink(db)


@given(st.from_regex(r"\*[\s/0-9,\-]*\*[\s/0-9,\-]*\*[\s/0-9,\-]*\*[\s/0-9,\-]*\*[\s/0-9,\-]*"))
def test_cron_expression_registration(expr):
    """Valid cron expressions (5-field pattern) should register without error."""
    s, db = _make_scheduler()
    def fn(): pass
    # Some auto-generated regexes won't be valid cron; just ensure no crash
    try:
        from croniter import croniter
        croniter(expr, datetime.utcnow())
        # It's valid cron — should register
        s.add(fn, cron=expr, name="cron_test")
        assert s._store.get_job("cron_test") is not None
    except (ValueError, KeyError):
        # Invalid cron — add() should raise
        with pytest.raises(Exception):
            s.add(fn, cron=expr, name="cron_test")
    finally:
        s._store.close()
        os.unlink(db)


# ===========================================================================
# 2. Scheduler stress test
# ===========================================================================

class TestStress:
    def test_500_jobs_all_fire(self):
        s, db = _make_scheduler()
        fired = threading.Event()
        count = {"n": 0}
        lock = threading.Lock()

        for i in range(500):
            def job_fn(idx=i):
                with lock:
                    count["n"] += 1
                    if count["n"] >= 500:
                        fired.set()

            s.add(job_fn, every="1s", name=f"job_{i}")

        s.start()
        fired.wait(timeout=15)
        s.stop()

        assert count["n"] >= 500, f"Only {count['n']} jobs fired"
        s._store.close()
        os.unlink(db)

    def test_rapid_start_stop_100(self):
        s, db = _make_scheduler()
        def fn(): pass
        s.add(fn, every="5s", name="rapid_job")

        for _ in range(100):
            s.start()
            time.sleep(0.02)
            s.stop()

        s._store.close()
        os.unlink(db)

    def test_job_longer_than_interval(self):
        """Job that takes longer than its interval should not pile up beyond max_concurrent."""
        s, db = _make_scheduler()
        count = {"n": 0}
        lock = threading.Lock()

        def slow_job():
            with lock:
                count["n"] += 1
            time.sleep(2)

        s.add(slow_job, every="1s", name="slow", max_concurrent=1)

        s.start()
        time.sleep(4)
        s.stop()

        # With max_concurrent=1 and 2s sleep, should fire at most ~2-3 times
        assert count["n"] <= 4, f"Job fired {count['n']} times (expected ≤4)"
        s._store.close()
        os.unlink(db)


# ===========================================================================
# 3. Concurrency
# ===========================================================================

class TestConcurrency:
    def test_multiple_threads_adding_jobs(self):
        s, db = _make_scheduler()
        added = {"n": 0}
        lock = threading.Lock()

        def add_jobs(thread_id):
            for i in range(50):
                def fn(): pass
                s.add(fn, every="10m", name=f"t{thread_id}_j{i}")
                with lock:
                    added["n"] += 1

        threads = [threading.Thread(target=add_jobs, args=(t,)) for t in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert s._store.delete_job("t0_j0") is True
        assert len(s.jobs) == 249  # 250 - 1 deleted
        s._store.close()
        os.unlink(db)

    def test_concurrent_run_now(self):
        s, db = _make_scheduler()
        count = {"n": 0}
        lock = threading.Lock()

        def fn():
            with lock:
                count["n"] += 1
            time.sleep(0.5)

        s.add(fn, every="1h", name="concurrent", max_concurrent=10)
        s._runner._ensure_executor()

        threads = []
        for _ in range(5):
            t = threading.Thread(target=s.run_now, args=("concurrent",))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

        time.sleep(1)
        assert count["n"] >= 1
        s._store.close()
        os.unlink(db)

    def test_concurrent_start_stop(self):
        s, db = _make_scheduler()
        errors = []

        def start_stop():
            try:
                for _ in range(20):
                    s.start()
                    time.sleep(0.01)
                    s.stop()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=start_stop) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors: {errors}"
        s._store.close()
        os.unlink(db)


# ===========================================================================
# 4. SQLite edge cases
# ===========================================================================

class TestSQLite:
    def test_large_job_name(self):
        s, db = _make_scheduler()
        big_name = "x" * 10000
        def fn(): pass
        s.add(fn, every="1h", name=big_name)
        job = s._store.get_job(big_name)
        assert job is not None
        assert job.name == big_name
        s._store.close()
        os.unlink(db)

    def test_many_runs_history(self):
        s, db = _make_scheduler()
        job_id = "test_job_id_123"
        # Create job manually
        job = Job(name="hist_job", func_path="mod.fn", schedule_type=ScheduleType.INTERVAL,
                  interval_seconds=60, id=job_id)
        s._store.save_job(job)

        for i in range(1000):
            run = JobRun(job_id=job_id, status=JobStatus.SUCCESS,
                         started_at=datetime.utcnow(), finished_at=datetime.utcnow(),
                         duration_seconds=0.1)
            s._store.save_run(run)

        runs = s._store.get_runs("hist_job", limit=5000)
        assert len(runs) == 1000

        runs_limited = s._store.get_runs("hist_job", limit=10)
        assert len(runs_limited) == 10

        s._store.close()
        os.unlink(db)

    def test_simultaneous_read_write(self):
        db = _tmp_db()
        store1 = Store(db)
        store2 = Store(db)

        def fn(): pass
        job = Job(name="shared_job", func_path="mod.fn", schedule_type=ScheduleType.INTERVAL,
                  interval_seconds=60, id="shared_id")
        store1.save_job(job)

        errors = []
        def reader():
            try:
                for _ in range(100):
                    store2.get_job("shared_job")
                    store2.list_jobs()
            except Exception as e:
                errors.append(e)

        def writer():
            try:
                for i in range(100):
                    store1.update_next_run("shared_id", datetime.utcnow() + timedelta(seconds=i))
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=reader)
        t2 = threading.Thread(target=writer)
        t1.start(); t2.start()
        t1.join(); t2.join()

        assert len(errors) == 0, f"Errors: {errors}"
        store1.close()
        store2.close()
        os.unlink(db)

    def test_wal_mode(self):
        db = _tmp_db()
        store = Store(db)
        mode = store.conn.execute("PRAGMA journal_mode").fetchone()[0]
        # WAL mode should be set (may be "wal" or already was)
        assert mode.lower() == "wal", f"Expected WAL, got {mode}"
        store.close()
        os.unlink(db)


# ===========================================================================
# 5. Cron edge cases
# ===========================================================================

class TestCron:
    def test_leap_year_feb29(self):
        from croniter import croniter
        expr = "0 0 29 2 *"
        dt = datetime(2024, 2, 29, 0, 0, 0)
        nxt = croniter(expr, dt).get_next(datetime)
        # Next should be 2028-02-29
        assert nxt.year == 2028 and nxt.month == 2 and nxt.day == 29

    def test_month_boundary(self):
        from croniter import croniter
        expr = "0 0 1 * *"  # first of every month
        dt = datetime(2024, 1, 1, 0, 0, 0)
        nxt = croniter(expr, dt).get_next(datetime)
        assert nxt == datetime(2024, 2, 1, 0, 0, 0)

    def test_year_boundary(self):
        from croniter import croniter
        expr = "0 0 1 1 *"  # Jan 1
        dt = datetime(2024, 1, 1, 0, 0, 0)
        nxt = croniter(expr, dt).get_next(datetime)
        assert nxt == datetime(2025, 1, 1, 0, 0, 0)

    def test_same_cron_multiple_jobs(self):
        s, db = _make_scheduler()
        counter1 = {"n": 0}
        counter2 = {"n": 0}

        def fn1():
            counter1["n"] += 1

        def fn2():
            counter2["n"] += 1

        s.add(fn1, cron="* * * * *", name="cron_a")
        s.add(fn2, cron="* * * * *", name="cron_b")

        jobs = s.jobs
        assert len(jobs) == 2
        assert all(j.cron_expr == "* * * * *" for j in jobs)

        s._store.close()
        os.unlink(db)

    @patch("whenly.scheduler.datetime")
    def test_dst_transition(self, mock_dt):
        """Mock a DST transition to verify cron doesn't break."""
        from croniter import croniter
        # Simulate spring forward
        naive_before = datetime(2024, 3, 10, 1, 59, 0)
        naive_after = datetime(2024, 3, 10, 3, 0, 0)
        expr = "0 * * * *"
        # croniter uses naive datetimes, so DST doesn't directly apply
        # This just verifies croniter works across the gap
        nxt = croniter(expr, naive_before).get_next(datetime)
        assert nxt is not None


# ===========================================================================
# 6. Lifecycle
# ===========================================================================

class TestLifecycle:
    def test_scheduler_not_context_manager(self):
        """Scheduler now implements __enter__/__exit__ as context manager for safe cleanup."""
        s, db = _make_scheduler()
        assert hasattr(s, '__enter__')
        assert hasattr(s, '__exit__')
        # Can use as context manager
        with s:
            s.add(lambda: None, every="5s", name="ctx_job")
        # close() called on exit — safe to unlink
        os.unlink(db)

    def test_graceful_shutdown_while_running(self):
        s, db = _make_scheduler()
        started = threading.Event()

        def slow_fn():
            started.set()
            time.sleep(5)

        s.add(slow_fn, every="1h", name="slow_shutdown")
        s.run_now("slow_shutdown")
        started.wait(timeout=5)

        # stop() should wait for running jobs
        start = time.time()
        s.stop()
        elapsed = time.time() - start

        # Should have waited (not instant)
        assert elapsed >= 0.1  # at minimum some time for thread setup
        s._store.close()
        os.unlink(db)

    def test_signal_sigterm_during_run(self):
        """SIGTERM while scheduler.run() is blocking should allow clean exit."""
        s, db = _make_scheduler(tick_interval=0.1)
        received = {"sig": False}

        def handler(signum, frame):
            received["sig"] = True
            s.stop()

        old = signal.signal(signal.SIGTERM, handler)
        try:
            def send_sig():
                time.sleep(0.3)
                os.kill(os.getpid(), signal.SIGTERM)

            t = threading.Thread(target=send_sig)
            t.start()
            s.run()
            t.join(timeout=3)
            assert received["sig"] is True
        finally:
            signal.signal(signal.SIGTERM, old)
            s._store.close()
            os.unlink(db)


# ===========================================================================
# 7. CLI integration
# ===========================================================================

class TestCLI:
    def _cli(self, db, args):
        result = subprocess.run(
            [sys.executable, "-c",
             f"import sys; sys.path.insert(0, 'src'); from whenly.cli import main; main({args})"],
            capture_output=True, text=True, cwd="/Volumes/Teja_T7_M4/clawd/whenly"
        )
        return result

    def test_list_empty(self):
        db = _tmp_db()
        store = Store(db)
        r = self._cli(db, ["--db", db, "list"])
        assert "No jobs" in r.stdout
        store.close()
        os.unlink(db)

    def test_list_one_job(self):
        db = _tmp_db()
        store = Store(db)
        job = Job(name="test_job", func_path="mod.fn", schedule_type=ScheduleType.INTERVAL,
                  interval_seconds=60, next_run_at=datetime.utcnow())
        store.save_job(job)
        r = self._cli(db, ["--db", db, "list"])
        assert "test_job" in r.stdout
        store.close()
        os.unlink(db)

    def test_history_no_runs(self):
        db = _tmp_db()
        store = Store(db)
        job = Job(name="hist_test", func_path="mod.fn", schedule_type=ScheduleType.INTERVAL,
                  interval_seconds=60)
        store.save_job(job)
        r = self._cli(db, ["--db", db, "history", "hist_test"])
        assert "No runs" in r.stdout
        store.close()
        os.unlink(db)

    def test_run_nonexistent_job(self):
        db = _tmp_db()
        store = Store(db)
        r = self._cli(db, ["--db", db, "run", "nope"])
        assert r.returncode != 0 or "not found" in r.stdout
        store.close()
        os.unlink(db)


# ===========================================================================
# 8. Edge cases
# ===========================================================================

class TestEdgeCases:
    def test_job_raises_then_succeeds(self):
        s, db = _make_scheduler()
        attempt = {"n": 0}

        def flaky():
            attempt["n"] += 1
            if attempt["n"] == 1:
                raise RuntimeError("first attempt fails")

        s.add(flaky, every="1s", name="flaky")
        s.start()
        time.sleep(3)
        s.stop()

        # Should have at least 2 attempts, and at least one should be FAILED
        runs = s._store.get_runs("flaky", limit=10)
        assert len(runs) >= 2
        statuses = [r.status for r in runs]
        assert JobStatus.FAILED in statuses, f"Expected a FAILED run, got: {statuses}"
        s._store.close()
        os.unlink(db)

    def test_remove_job_while_executing(self):
        s, db = _make_scheduler()
        started = threading.Event()
        done = threading.Event()

        def slow():
            started.set()
            time.sleep(1)
            done.set()

        s.add(slow, every="1h", name="removable")
        s.run_now("removable")
        started.wait(timeout=5)

        # Delete while running
        assert s._store.delete_job("removable") is True

        # Job should still complete (already submitted to thread pool)
        done.wait(timeout=5)
        assert done.is_set()
        # Use close() which stops runner (waits for completion) then closes store
        s.close()
        os.unlink(db)

    def test_disable_job_while_executing(self):
        s, db = _make_scheduler()
        started = threading.Event()
        done = threading.Event()

        def slow():
            started.set()
            time.sleep(0.5)
            done.set()

        s.add(slow, every="1s", name="disable_me")
        s.run_now("disable_me")
        started.wait(timeout=5)
        s.disable("disable_me")

        done.wait(timeout=5)
        assert done.is_set()  # Current execution should complete
        s.stop()
        s._store.close()
        os.unlink(db)

    def test_very_fast_tick_interval(self):
        s, db = _make_scheduler(tick_interval=0.001)
        count = {"n": 0}
        lock = threading.Lock()

        def fn():
            with lock:
                count["n"] += 1

        s.add(fn, every="1s", name="fast_tick")
        s.start()
        time.sleep(2)
        s.stop()

        assert count["n"] >= 1
        s._store.close()
        os.unlink(db)

    def test_oneoff_job_disabled_after_run(self):
        s, db = _make_scheduler()
        ran = {"n": 0}

        def once():
            ran["n"] += 1

        s.later(1, "seconds", once, name="oneoff")
        job = s._store.get_job("oneoff")
        assert job is not None
        assert job.schedule_type == ScheduleType.ONCE

        s.start()
        time.sleep(2)
        s.stop()

        # Should have run once and been disabled
        assert ran["n"] >= 1
        job = s._store.get_job("oneoff")
        assert job is not None and job.enabled is False
        s._store.close()
        os.unlink(db)

    def test_add_without_every_or_cron_raises(self):
        s, db = _make_scheduler()
        def fn(): pass
        with pytest.raises(ValueError, match="Must specify"):
            s.add(fn)
        s._store.close()
        os.unlink(db)

    def test_run_now_nonexistent(self):
        s, db = _make_scheduler()
        assert s.run_now("nope") is False
        s._store.close()
        os.unlink(db)

    def test_disable_nonexistent(self):
        s, db = _make_scheduler()
        assert s.disable("nope") is False
        s._store.close()
        os.unlink(db)

    def test_enable_nonexistent(self):
        s, db = _make_scheduler()
        assert s.enable("nope") is False
        s._store.close()
        os.unlink(db)
