init:
	poetry install
	poetry run pre-commit install
	meltano install
local-es:
	docker-compose up -d
test:
	poetry run pytest
lint:
	poetry run pre-commit run --all-files
