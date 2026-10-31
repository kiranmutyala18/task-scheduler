import time
from threading import Event
import pytest
from scheduler import TaskScheduler, TaskState

def test_runs_a_task_and_returns_result():
    scheduler = TaskScheduler()
    try:
        assert scheduler.schedule(lambda: 2 + 3).result() == 5
    finally:
        scheduler.shutdown()

def test_delays_execution():
    scheduler = TaskScheduler()
    started = time.monotonic()
    try:
        scheduler.schedule(lambda: None, delay=0.05).result()
        assert time.monotonic() - started >= 0.04
    finally:
        scheduler.shutdown()

def test_task_exception_reaches_caller():
    scheduler = TaskScheduler()
    try:
        handle = scheduler.schedule(lambda: 1 / 0)
        with pytest.raises(ZeroDivisionError):
            handle.result()
        assert handle.state is TaskState.FAILED
    finally:
        scheduler.shutdown()

def test_cancelled_task_does_not_run():
    scheduler = TaskScheduler()
    ran = Event()
    try:
        handle = scheduler.schedule(ran.set, delay=1)
        assert scheduler.cancel(handle)
        with pytest.raises(RuntimeError):
            handle.result()
        assert not ran.is_set()
    finally:
        scheduler.shutdown()

def test_recurring_task_runs_again():
    scheduler = TaskScheduler()
    calls = []
    try:
        recurring = scheduler.schedule_every(lambda: calls.append(1), interval=0.02)
        deadline = time.monotonic() + 0.5
        while len(calls) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(calls) >= 2
        assert recurring.cancel()
    finally:
        scheduler.shutdown()
