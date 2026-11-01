import time
from threading import Event

import pytest

from scheduler import TaskScheduler, TaskState


def test_runs_a_task_and_returns_its_result() -> None:
    scheduler = TaskScheduler()
    try:
        assert scheduler.schedule(lambda: 2 + 3).result() == 5
    finally:
        scheduler.shutdown()


def test_delays_task_execution() -> None:
    scheduler = TaskScheduler()
    started = time.monotonic()
    try:
        scheduler.schedule(lambda: None, delay=0.05).result()
        assert time.monotonic() - started >= 0.04
    finally:
        scheduler.shutdown()


def test_task_exception_reaches_caller() -> None:
    scheduler = TaskScheduler()
    try:
        handle = scheduler.schedule(lambda: 1 / 0)
        with pytest.raises(ZeroDivisionError):
            handle.result()
        assert handle.state is TaskState.FAILED
    finally:
        scheduler.shutdown()


def test_cancelled_task_does_not_run() -> None:
    scheduler = TaskScheduler()
    ran = Event()
    try:
        handle = scheduler.schedule(ran.set, delay=1)
        assert scheduler.cancel(handle) is True
        with pytest.raises(RuntimeError, match="cancelled"):
            handle.result()
        assert ran.is_set() is False
    finally:
        scheduler.shutdown()


def test_two_workers_can_start_two_tasks() -> None:
    scheduler = TaskScheduler(worker_count=2)
    first_started = Event()
    second_started = Event()
    release = Event()
    try:
        first = scheduler.schedule(lambda: (first_started.set(), release.wait())[1])
        second = scheduler.schedule(lambda: (second_started.set(), release.wait())[1])
        assert first_started.wait(1)
        assert second_started.wait(1)
        release.set()
        first.result()
        second.result()
    finally:
        scheduler.shutdown()


def test_recurring_task_runs_again_until_cancelled() -> None:
    scheduler = TaskScheduler()
    calls: list[int] = []
    try:
        recurring = scheduler.schedule_every(
            lambda: calls.append(1), interval=0.02
        )
        deadline = time.monotonic() + 0.5
        while len(calls) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(calls) >= 2
        assert recurring.cancel() is True
    finally:
        scheduler.shutdown()


def test_stats_count_finished_and_failed_tasks() -> None:
    scheduler = TaskScheduler()
    try:
        scheduler.schedule(lambda: "done").result()
        failed = scheduler.schedule(lambda: 1 / 0)
        with pytest.raises(ZeroDivisionError):
            failed.result()
        stats = scheduler.stats()
        assert stats.succeeded == 1
        assert stats.failed == 1
    finally:
        scheduler.shutdown()
