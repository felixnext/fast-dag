# Fast DAG

A fast DAG library for Python.


## Development

Everything is managed with [uv](https://docs.astral.sh/uv/).

Install pre-commit hooks:

```bash
uv run pre-commit install
```

Run tests:

```bash
uv run pytest
```

Run linting:

```bash
uv run ruff check .
uv run ruff format .
```

Run type checking:

```bash
uv run mypy fast-dag
```
