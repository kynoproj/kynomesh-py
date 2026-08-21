.PHONY: all
all: test

.PHONY: sync
sync:
	uv sync --group dev

.PHONY: test
test: sync
	uv run pytest

.PHONY: test-coverage
test-coverage: sync
	uv run pytest --cov=src --cov-report=xml --cov-report=term-missing

.PHONY: lint
lint: sync
	uv run ruff check --fix .
	uv run ruff format .

.PHONY: lint-check
lint-check: sync
	uv run ruff check .
	uv run ruff format --check .
