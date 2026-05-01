.PHONY: init local-es test lint format

init:
	uv sync
	uv run pre-commit install
	meltano install

local-es:
	docker compose up -d

test:
	uv run pytest

lint:
	uv run pre-commit run --all-files

format:
	uv run black target_elasticsearch tests
	uv run isort target_elasticsearch tests
