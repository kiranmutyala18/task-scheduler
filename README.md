# Task scheduler

A Python task scheduler with a priority queue, one dispatcher thread, and a fixed worker pool.

## Design

The dispatcher sleeps until the next task's deadline, then moves it to a thread-safe ready queue. Workers consume that queue and publish a task result, error, or cancellation status through a `TaskHandle`.

## V2 additions

- `schedule_every()` repeats a task until its `RecurringTask` is cancelled.
- `stats()` reports pending, running, successful, failed, and cancelled tasks.
- Run `python cli.py "generate report" --delay 2` for a command-line demonstration.

## V3 service layer

The worker-pool core remains based on `heapq` and threads. A FastAPI boundary now accepts serializable echo jobs at `POST /v1/tasks`, and SQLite restores pending API-created jobs after a restart. Logs are JSON records. Use `python cli.py submit "report" --delay 5` or `python cli.py list` for the Typer CLI.

## Run tests

```powershell
python -m pytest
```
