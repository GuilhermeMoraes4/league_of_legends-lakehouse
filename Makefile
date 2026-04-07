.PHONY: up down logs restart ps build test lint fmt type-check install-dev pre-commit-install

AIRFLOW_DIR = airflow

# ── Docker / Airflow ──────────────────────────────────────────────────────────

build:
	cd $(AIRFLOW_DIR) && docker-compose build

up:
	cd $(AIRFLOW_DIR) && docker-compose up -d

down:
	cd $(AIRFLOW_DIR) && docker-compose down

logs:
	cd $(AIRFLOW_DIR) && docker-compose logs -f

restart:
	cd $(AIRFLOW_DIR) && docker-compose restart

ps:
	cd $(AIRFLOW_DIR) && docker-compose ps

# ── Python ────────────────────────────────────────────────────────────────────

install-dev:
	pip install -e ".[dev]"

pre-commit-install:
	pre-commit install

test:
	pytest tests/ -v

lint:
	ruff check src/ tests/

fmt:
	ruff format src/ tests/

type-check:
	mypy src/
