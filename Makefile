# Raccourcis d'exploitation. Chaque cible n'est qu'un alias de commande Docker
# Compose (voir README, section « Commandes utiles », pour les équivalents sans make).

COMPOSE   ?= docker compose
PYTHON    ?= python
SCHEDULER := $(COMPOSE) exec airflow-scheduler
PROJECT   := /opt/airflow/project
START     ?=
END       ?=

.PHONY: help env up down run status logs smoke verify test test-unit test-integration test-api lint format \
        typecheck dev-install trino-cli clean rebuild

help: ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

env: ## Génère .env avec des secrets aléatoires (sans écraser un .env existant)
	$(PYTHON) scripts/generate_env.py

.env:
	$(PYTHON) scripts/generate_env.py

up: .env ## Construit et démarre la stack (le pipeline se lance automatiquement)
	$(COMPOSE) up -d --build

down: ## Arrête la stack (données conservées)
	$(COMPOSE) down

clean: ## Arrête la stack ET supprime toutes les données (volumes)
	$(COMPOSE) down -v --remove-orphans

rebuild: ## Reconstruit les images sans cache
	$(COMPOSE) build --no-cache

run: ## Déclenche le DAG e2e (optionnel : make run START=2023-02-01 END=2023-03-01)
ifeq ($(START)$(END),)
	$(SCHEDULER) airflow dags trigger chicago_taxi_e2e
else
	$(SCHEDULER) airflow dags trigger chicago_taxi_e2e --conf '{"start_date": "$(START)", "end_date": "$(END)"}'
endif

status: ## État des derniers runs du DAG e2e
	$(SCHEDULER) airflow dags list-runs -d chicago_taxi_e2e

logs: ## Logs du scheduler (exécution des tâches)
	$(COMPOSE) logs -f airflow-scheduler

verify: ## Vérifie toute l'installation (services, pipeline, données, dashboard, tests)
	$(PYTHON) scripts/verify_e2e.py --with-tests

smoke: ## Vérifie les tables via Trino et affiche un aperçu des KPIs
	$(SCHEDULER) python $(PROJECT)/scripts/smoke_test.py

test: test-unit test-integration test-api ## Tous les tests (dans les conteneurs)

test-unit: ## Tests unitaires (Spark local, sans S3) dans le conteneur
	$(SCHEDULER) bash -c "cd $(PROJECT) && python -m pytest tests/unit -q"

test-integration: ## Tests d'intégration sur la stack (round-trip Spark/Trino, idempotence)
	$(SCHEDULER) bash -c "cd $(PROJECT) && RUN_INTEGRATION=1 python -m pytest tests/integration -q"

test-api: ## Tests de l'API gold (dans le conteneur api)
	$(COMPOSE) exec api python -m pytest -q

trino-cli: ## Ouvre le CLI Trino sur le catalogue iceberg
	$(COMPOSE) exec trino trino --catalog iceberg

dev-install: ## Environnement de dev local (lint, typage, tests unitaires)
	python -m pip install -e ".[dev]"

lint: ## ruff (lint + format check) + mypy strict
	python -m ruff check src dags tests scripts api
	python -m ruff format --check src dags tests scripts api
	python -m mypy

format: ## Formate le code
	python -m ruff format src dags tests scripts api
	python -m ruff check --fix src dags tests scripts api

typecheck: ## mypy strict
	python -m mypy
