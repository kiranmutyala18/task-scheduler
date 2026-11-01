from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from service import PersistentTaskService


class EchoTask(BaseModel):
    message: str = Field(min_length=1, max_length=2000)
    delay_seconds: float = Field(default=0, ge=0, le=86400)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.service = PersistentTaskService()
    yield
    app.state.service.close()


app = FastAPI(title="Task Scheduler API", lifespan=lifespan)


@app.post("/v1/tasks", status_code=201)
async def submit(task: EchoTask) -> dict[str, object]:
    task_id = app.state.service.submit_echo(task.message, task.delay_seconds)
    return {"task_id": task_id, "status": "pending"}


@app.get("/v1/tasks/{task_id}")
async def get_task(task_id: int) -> dict[str, object]:
    result = app.state.service.get(task_id)
    if result is None:
        raise HTTPException(status_code=404, detail="task not found")
    return result


@app.get("/v1/tasks")
async def list_tasks() -> list[dict[str, object]]:
    return app.state.service.recent()
