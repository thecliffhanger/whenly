# whenly Code Review

## Summary

A clean, well-structured lightweight job scheduler. The code is readable and the API is intuitive. Below are findings organized by severity.

---

## 🔴 Bugs / Correctness

### 1. `del self._futures[run.id]` is not thread-safe (runner.py:70)
`_futures` is a plain `dict` accessed from the scheduler's tick loop thread **and** worker threads. No lock protects it. A crash in `_execute` before reaching `del` leaks entries; concurrent access can corrupt the dict.

**Fix:** Add a `threading.Lock` around all `_futures` access, or use `concurrent.futures` callbacks instead of manual cleanup.

### 2. Timeout doesn't actually kill the job (runner.py:55-58)
```python
result = func()
if isinstance(result, Future):
    result.result(timeout=job.timeout_seconds)
```
This only works if the job itself returns a `Future`. For a normal function like `time.sleep(5)`, nothing enforces the timeout — the thread blocks forever. `func()` is called **before** any timeout check.

**Fix:** Use a wrapper with `signal.alarm` (Unix only) or run the job via its own `Future` with a timeout, or use `concurrent.futures.ThreadPoolExecutor.submit().result(timeout=...)`.

### 3. `RUN_ALL` missed runs submit jobs with stale `next_run_at` (scheduler.py:153-158)
When submitting missed jobs, the copied Job retains the original `next_run_at`. These will be picked up again on the next tick as "due" jobs, causing duplicate executions.

**Fix:** Set `next_run_at` far in the future (like the ONCE case) for missed catch-up jobs, or pass a flag to skip them.

### 4. One-off jobs resurface after scheduler restart (scheduler.py:143)
`ONCE` jobs are pushed 100 years into the future instead of being disabled/deleted. After a restart, they'll still appear in `list_jobs` and have `next_run_at` in year 2126.

**Fix:** Set `enabled=False` after execution, or delete the job.

### 5. `_parse_interval_str` doesn't handle `"ms"` returning float (scheduler.py:195-196)
`"100ms"` → `int(100 * 0.001)` → `0` due to `int()` truncation. The return type is `int` but the multiplier produces a float.

**Fix:** Change return type to `float` or remove `"ms"` support.

### 6. `get_missed_count` off-by-one (store.py:112)
```python
count = int(elapsed // job.interval_seconds)
return max(0, count - 1) if job.next_run_at else max(0, count)
```
The logic is fragile. If `last_run_at` was 10s ago with interval 5s, `elapsed//5 = 2`, returns `1`. But the "current" run (already submitted) accounts for one, so this is arguably correct — but only by accident. The conditional on `next_run_at` makes it inconsistent.

---

## 🟡 Thread Safety

### 7. SQLite `check_same_thread=False` without locking (store.py:41)
All threads share one SQLite connection. SQLite in WAL mode supports concurrent reads but only one writer. With `isolation_level=None` (autocommit), individual statements are atomic, but multi-step operations (e.g., `save_job` then `get_job`) are not. This can lead to race conditions under load.

**Fix:** Either use a connection per thread, or add a write lock around multi-step operations.

### 8. `_funcs` dict unprotected in Scheduler (scheduler.py:71)
`_funcs` is written at registration time and read in the scheduler loop + worker threads. While Python's GIL makes plain dict access crash-safe, the visibility of newly-added entries isn't guaranteed without synchronization.

---

## 🟡 Resource Leaks

### 9. `ThreadPoolExecutor` not cleaned up on exception in `_loop` (runner.py)
If `_loop` crashes (e.g., unhandled exception in `_tick`), `shutdown()` is never called. The daemon thread makes this non-blocking but leaks threads.

### 10. Store never closed in decorator usage (scheduler.py)
If users only use decorators and call `scheduler.start()`, `store.close()` is never called. Minor for short-lived processes, but a concern for long-running services.

---

## 🟢 API / Design

### 11. `later()` decorator returns `Job` instead of `Callable` (scheduler.py:113)
```python
@s.later(5, "minutes")
def cleanup(): ...  # cleanup is now a Job, not the function!
```
The docstring says it can be used as a decorator, but the return value replaces the function with a `Job` object. This is a design inconsistency — `every()` and `cron()` return the original function.

### 12. `args`/`kwargs` in `later()` are accepted but never used (scheduler.py:101-102)
Parameters `args` and `kwargs` are in the signature but `_create_oneoff` ignores them.

### 13. No way to pass arguments to jobs
Jobs are always called as `func()` with no arguments. Consider supporting `args`/`kwargs` in the `Job` model.

### 14. `Job.func` is `Any` but should be `Callable[..., Any] | None` (models.py:53)

---

## 🟢 SQL Injection

✅ **No issues found.** All queries use parameterized placeholders. Good.

---

## 🟢 Error Handling

### 15. `_execute` silently swallows function resolution failure (runner.py:46-49)
If `func` is `None` and `_resolve_func` fails, the job is marked `FAILED` but only with a debug log. The user has no visibility unless they check history.

### 16. `save_job` falls back to the input `job` if `get_job` returns `None` (store.py:71)
This shouldn't happen (just inserted), but if it does, the returned `Job` lacks fields populated by the DB. Could mask data issues.

---

## 🟢 Type Safety

### 17. `_fmt_schedule` has no type hint for `job` parameter (cli.py:83)

### 18. `Job.func: Any` should be `Callable[..., Any] | None` (models.py:53)

---

## 🟢 Code Style

### 19. Docstring examples inconsistent with actual API
`every()` docstring shows `@s.every(10, minutes=True)` which isn't implemented.

### 20. Unused imports: `datetime`, `timezone` in cli.py (only `datetime` needed, `timezone` unused)

---

## Tests Assessment

- Good coverage of happy paths (interval, cron, one-off, concurrency, enable/disable)
- **Missing:** No tests for missed-run backfill (`RUN_ALL`), error propagation from failing jobs, scheduler restart persistence, concurrent write stress, or the actual timeout enforcement
- Test for `run_blocking` relies on `scheduler.stop()` being called from within a job — fragile but functional
- `test_add_programmatic` uses `every="500s"` which won't fire in 1.5s — the job runs because `next_run_at` is set to `utcnow()` at registration time. Test name is misleading

---

## Recommendations (Priority Order)

1. **Fix the timeout enforcement** (#2) — it's the most user-facing bug
2. **Lock `_futures`** (#1) — potential data corruption
3. **Fix `RUN_ALL` duplicate submission** (#3) — causes actual wrong behavior
4. **Fix `later()` decorator return type** (#11) — breaks the `@` syntax
5. **Add write lock or connection-per-thread** (#7) — correctness under load
6. **Clean up one-off jobs after execution** (#4)
