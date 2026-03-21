"""CLI for whenly."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from .models import JobStatus
from .store import Store


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="whenly", description="Lightweight job scheduler")
    parser.add_argument("--db", default="whenly.db", help="Database path")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("list", help="List all jobs and next run time")
    sub.add_parser("history", help="Show job run history").add_argument("name")

    run_parser = sub.add_parser("run", help="Manually trigger a job")
    run_parser.add_argument("name")

    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        sys.exit(1)

    store = Store(args.db)

    if args.command == "list":
        jobs = store.list_jobs()
        if not jobs:
            print("No jobs registered.")
            return
        for j in jobs:
            next_run = j.next_run_at.strftime("%Y-%m-%d %H:%M:%S") if j.next_run_at else "—"
            status = "✓" if j.enabled else "✗"
            sched = _fmt_schedule(j)
            print(f"  {status} {j.name:<30} [{sched:<12}] next: {next_run}")

    elif args.command == "history":
        runs = store.get_runs(args.name, limit=20)
        if not runs:
            print(f"No runs found for '{args.name}'.")
            return
        for r in runs:
            ts = r.started_at.strftime("%Y-%m-%d %H:%M:%S") if r.started_at else "?"
            dur = f"{r.duration_seconds:.1f}s" if r.duration_seconds else "?"
            status_icon = {
                JobStatus.SUCCESS: "✓",
                JobStatus.FAILED: "✗",
                JobStatus.TIMEOUT: "⏱",
                JobStatus.RUNNING: "⟳",
            }.get(r.status, "?")
            err = f" — {r.error_message.splitlines()[-1][:60]}" if r.error_message else ""
            print(f"  {status_icon} {ts}  {dur:>8}  {r.status.value:<8}{err}")

    elif args.command == "run":
        # For run, we need to try to import and execute the function
        job = store.get_job(args.name)
        if not job:
            print(f"Job '{args.name}' not found.")
            sys.exit(1)
        try:
            parts = job.func_path.rsplit(".", 1)
            if len(parts) == 2:
                import importlib
                mod = importlib.import_module(parts[0])
                func = getattr(mod, parts[1])
                print(f"Running {args.name}...")
                func()
                print("Done.")
            else:
                print(f"Cannot resolve function: {job.func_path}")
                sys.exit(1)
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

    store.close()


def _fmt_schedule(job) -> str:
    if job.schedule_type.value == "interval":
        return f"every {job.interval_seconds}s"
    elif job.schedule_type.value == "cron":
        return job.cron_expr or "?"
    elif job.schedule_type.value == "once":
        return f"once at {job.scheduled_for or '?'}"
    return job.schedule_type.value


if __name__ == "__main__":
    main()
