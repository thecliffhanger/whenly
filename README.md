# whenly

[![PyPI version](https://badge.fury.io/py/whenly.svg)](https://pypi.org/project/whenly)
[![Python versions](https://img.shields.io/pypi/pyversions/whenly.svg)](https://pypi.org/project/whenly)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Lightweight persistent job scheduler for Python — SQLite-backed, minimal dependencies.

```python
from whenly import Scheduler

s = Scheduler()

@s.every(5, minutes)
def sync_data():
    ...

@s.cron("0 9 * * MON")
def weekly_report():
    ...

s.start()  # background thread
```

## Features

- **SQLite persistence** — jobs survive restarts
- **Cron expressions** via croniter
- **Interval scheduling** — every N seconds/minutes/hours
- **One-off delayed jobs** — run once after a delay
- **Decorator or programmatic API**
- **Thread-safe** background runner
- **CLI** for basic management
- **Zero external dependencies** (except croniter for cron support)

## Install

```bash
pip install whenly
```

## Quick Start

```python
from whenly import Scheduler

s = Scheduler()

# Interval jobs
@s.every(30, seconds)
def poll_api():
    print("Polling...")

# Cron jobs
@s.cron("0 */2 * * *")
def cleanup():
    print("Running cleanup...")

# One-off delayed job
s.later(10, minutes, send_notification, msg="Hello")

# Start the scheduler (non-blocking)
s.start()

# Or run blocking
# s.run()
```

### Programmatic API

```python
s.add_job("poll", interval=60, unit="seconds", fn=poll_api)
s.add_job("cleanup", cron="0 3 * * *", fn=cleanup)
s.remove_job("poll")
s.list_jobs()
```

## CLI

```bash
whenly list
whenly run
```

## License

MIT

---

Part of the [thecliffhanger](https://github.com/thecliffhanger) open source suite.
