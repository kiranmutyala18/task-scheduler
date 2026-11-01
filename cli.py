"""Typer CLI for persisted scheduler jobs."""

import json

import typer

from service import PersistentTaskService

app = typer.Typer(no_args_is_help=True)


@app.command()
def submit(message: str, delay: float = typer.Option(0, min=0)) -> None:
    service = PersistentTaskService()
    try:
        typer.echo(service.submit_echo(message, delay))
    finally:
        service.close()


@app.command("list")
def list_tasks() -> None:
    service = PersistentTaskService()
    try:
        typer.echo(json.dumps(service.recent(), indent=2))
    finally:
        service.close()


if __name__ == "__main__":
    app()
