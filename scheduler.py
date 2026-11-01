"""A delayed-task scheduler with a dispatcher thread and worker pool."""

from __future__ import annotations

import heapq
import itertools
import time
from dataclasses import dataclass, field
from enum import Enum
from queue import Queue
from threading import Condition, Event, Thread
from typing import Any, Callable


class TaskState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class SchedulerStats:
    pending: int
    running: int
    succeeded: int
    failed: int
    cancelled: int


@dataclass
class TaskHandle:
    """A caller-facing handle for a scheduled task."""

    task_id: int
    state: TaskState = TaskState.PENDING
    _done: Event = field(default_factory=Event, repr=False)
    _result: Any = field(default=None, repr=False)
    _error: BaseException | None = field(default=None, repr=False)

    def result(self, timeout: float | None = None) -> Any:
        if not self._done.wait(timeout):
            raise TimeoutError(f"task {self.task_id} did not finish in time")
        if self.state is TaskState.CANCELLED:
            raise RuntimeError(f"task {self.task_id} was cancelled")
        if self._error is not None:
            raise self._error
        return self._result


class RecurringTask:
    """Controls repeated execution of one callable."""

    def __init__(self, scheduler: "TaskScheduler") -> None:
        self._scheduler = scheduler
        self._cancelled = False
        self._current: TaskHandle | None = None

    def cancel(self) -> bool:
        self._cancelled = True
        return self._current is None or self._scheduler.cancel(self._current)

    def _set_current(self, handle: TaskHandle) -> None:
        self._current = handle
        if self._cancelled:
            self._scheduler.cancel(handle)


@dataclass
class _ScheduledTask:
    run_at: float
    handle: TaskHandle
    function: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


class TaskScheduler:
    """Schedules callables and runs them on a fixed-size worker pool."""

    def __init__(self, worker_count: int = 2) -> None:
        if worker_count < 1:
            raise ValueError("worker_count must be at least 1")
        self._condition = Condition()
        self._counter = itertools.count()
        self._scheduled: list[tuple[float, int, _ScheduledTask]] = []
        self._handles: dict[int, TaskHandle] = {}
        self._ready: Queue[_ScheduledTask | None] = Queue()
        self._accepting = True
        self._stopping = False
        self._dispatcher = Thread(target=self._dispatch, name="scheduler-dispatcher")
        self._workers = [
            Thread(target=self._work, name=f"scheduler-worker-{number}")
            for number in range(worker_count)
        ]
        self._dispatcher.start()
        for worker in self._workers:
            worker.start()

    def schedule(
        self, function: Callable[..., Any], *args: Any, delay: float = 0, **kwargs: Any
    ) -> TaskHandle:
        if delay < 0:
            raise ValueError("delay cannot be negative")
        with self._condition:
            if not self._accepting:
                raise RuntimeError("scheduler is shut down")
            task_id = next(self._counter)
            handle = TaskHandle(task_id)
            self._handles[task_id] = handle
            task = _ScheduledTask(time.monotonic() + delay, handle, function, args, kwargs)
            heapq.heappush(self._scheduled, (task.run_at, task_id, task))
            self._condition.notify()
            return handle

    def schedule_every(
        self,
        function: Callable[..., Any],
        *args: Any,
        interval: float,
        initial_delay: float = 0,
        **kwargs: Any,
    ) -> RecurringTask:
        """Run a callable repeatedly until its returned control is cancelled."""
        if interval <= 0:
            raise ValueError("interval must be greater than zero")
        control = RecurringTask(self)

        def run_again() -> None:
            function(*args, **kwargs)
            if control._cancelled:
                return
            try:
                control._set_current(self.schedule(run_again, delay=interval))
            except RuntimeError:
                # Shutdown stopped the scheduler between runs.
                return

        control._set_current(self.schedule(run_again, delay=initial_delay))
        return control

    def stats(self) -> SchedulerStats:
        """Return a point-in-time count of all task states."""
        with self._condition:
            states = [handle.state for handle in self._handles.values()]
        return SchedulerStats(
            pending=states.count(TaskState.PENDING),
            running=states.count(TaskState.RUNNING),
            succeeded=states.count(TaskState.SUCCEEDED),
            failed=states.count(TaskState.FAILED),
            cancelled=states.count(TaskState.CANCELLED),
        )

    def cancel(self, handle: TaskHandle) -> bool:
        with self._condition:
            if handle.state is not TaskState.PENDING:
                return False
            handle.state = TaskState.CANCELLED
            handle._done.set()
            self._condition.notify()
            return True

    def shutdown(self, wait: bool = True) -> None:
        with self._condition:
            self._accepting = False
            self._stopping = True
            for _, _, task in self._scheduled:
                if task.handle.state is TaskState.PENDING:
                    task.handle.state = TaskState.CANCELLED
                    task.handle._done.set()
            self._scheduled.clear()
            self._condition.notify_all()
        if wait:
            self._dispatcher.join()
            for worker in self._workers:
                worker.join()

    def _dispatch(self) -> None:
        while True:
            with self._condition:
                while not self._scheduled and not self._stopping:
                    self._condition.wait()
                if self._stopping:
                    break
                run_at, _, task = self._scheduled[0]
                delay = run_at - time.monotonic()
                if delay > 0:
                    self._condition.wait(delay)
                    continue
                heapq.heappop(self._scheduled)
                if task.handle.state is TaskState.PENDING:
                    self._ready.put(task)
        for _ in self._workers:
            self._ready.put(None)

    def _work(self) -> None:
        while True:
            task = self._ready.get()
            try:
                if task is None:
                    return
                with self._condition:
                    if task.handle.state is TaskState.CANCELLED:
                        continue
                    task.handle.state = TaskState.RUNNING
                try:
                    task.handle._result = task.function(*task.args, **task.kwargs)
                    task.handle.state = TaskState.SUCCEEDED
                except BaseException as error:
                    task.handle._error = error
                    task.handle.state = TaskState.FAILED
                finally:
                    task.handle._done.set()
            finally:
                self._ready.task_done()
