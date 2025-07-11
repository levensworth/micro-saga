lint:
	poetry run ruff format src
	poetry run ruff format tests
# 	poetry run ruff format examples

check-style:
	poetry run ruff check

run_types:
	poetry run mypy src
	poetry run mypy tests

test:
	poetry run pytest --cov=src --cov-report=xml tests 