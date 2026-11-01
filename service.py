"""SQLite persistence and structured logging around the plain-Python scheduler."""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

from scheduler import TaskScheduler, TaskState


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({"level": record.levelname, "message": record.getMessage()})


logger = logging.getLogger("task_scheduler")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


class PersistentTaskService:
    """Persists serializable echo jobs and restores pending work after restart."""

    def __init__(self, database_path: str | Path = "data/tasks.db") -> None:
        self._path = Path(database_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._scheduler = TaskScheduler()
        with self._connect() as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, run_at REAL NOT NULL, status TEXT NOT NULL, result TEXT)")
        with self._connect() as connection:
            rows = connection.execute("SELECT id, payload, run_at FROM tasks WHERE status = ?", (TaskState.PENDING.value,)).fetchall()
        for row in rows:
            self._schedule(int(row["id"]), float(row["run_at"]), json.loads(row["payload"])["message"])

    def submit_echo(self, message: str, delay_seconds: float = 0) -> int:
        if not message.strip() or delay_seconds < 0:
            raise ValueError("message must be non-empty and delay non-negative")
        run_at = time.time() + delay_seconds
        with self._connect() as connection:
            cursor = connection.execute("INSERT INTO tasks (payload, run_at, status) VALUES (?, ?, ?)", (json.dumps({"message": message}), run_at, TaskState.PENDING.value))
            task_id = int(cursor.lastrowid)
        self._schedule(task_id, run_at, message)
        return task_id

    def get(self, task_id: int) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return dict(row) if row else None

    def recent(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM tasks ORDER BY id DESC LIMIT 50").fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        self._scheduler.shutdown()

    def _schedule(self, task_id: int, run_at: float, message: str) -> None:
        def run() -> None:
            self._update(task_id, TaskState.RUNNING)
            logger.info("running task_id=%s", task_id)
            self._update(task_id, TaskState.SUCCEEDED, message)
        self._scheduler.schedule(run, delay=max(0, run_at - time.time()))

    def _update(self, task_id: int, state: TaskState, result: str | None = None) -> None:
        with self._connect() as connection:
            connection.execute("UPDATE tasks SET status = ?, result = ? WHERE id = ?", (state.value, result, task_id))

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path)
        connection.row_factory = sqlite3.Row
        return connection
