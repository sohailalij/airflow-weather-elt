.PHONY: init up down logs trigger query test lint clean

init:
	echo "AIRFLOW_UID=$$(id -u)" > .env

up: init
	docker compose up -d --build
	@echo "Airflow UI: http://localhost:8080  (admin / admin)"

down:
	docker compose down

logs:
	docker compose logs -f airflow-scheduler

trigger:
	docker compose exec airflow-scheduler airflow dags trigger weather_air_quality_elt

query:
	docker compose exec airflow-scheduler python /opt/airflow/scripts/query.py

test:
	pytest -v

lint:
	ruff check . && ruff format --check .

clean:
	docker compose down -v
	rm -rf data/raw data/*.duckdb logs/*
