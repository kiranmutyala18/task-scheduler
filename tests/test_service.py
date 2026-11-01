import time

from service import PersistentTaskService


def test_persists_and_executes_echo_task(tmp_path) -> None:
    service = PersistentTaskService(tmp_path / "tasks.db")
    try:
        task_id = service.submit_echo("hello")
        deadline = time.time() + 1
        while service.get(task_id)["status"] == "pending" and time.time() < deadline:
            time.sleep(0.01)
        assert service.get(task_id)["result"] == "hello"
    finally:
        service.close()
