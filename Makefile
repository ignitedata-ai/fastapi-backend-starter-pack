# AIPAL Backend Services Makefile

.PHONY: help install dev test lint format clean build up down logs shell init-db

# Default target
help:
	@echo "Available commands:"
	@echo "  install     - Install dependencies"
	@echo "  dev         - Run development server"
	@echo "  test        - Run tests"
	@echo "  test-cov    - Run tests with coverage"
	@echo "  lint        - Run linting"
	@echo "  format      - Format code"
	@echo "  clean       - Clean up generated files"
	@echo "  build       - Build Docker images"
	@echo "  up          - Start services with Docker Compose"
	@echo "  down        - Stop services"
	@echo "  logs        - View service logs"
	@echo "  shell       - Open shell in backend container"
	@echo "  migrate     - Run database migrations"
	@echo "  init-db     - Initialize database with seed data"
	@echo "  check       - Run all quality checks"

install:
	uv pip install -e .

dev:
	python main.py

test:
	pytest -v

test-cov:
	pytest --cov=core --cov=api --cov-report=html --cov-report=term

lint:
	ruff check .
	mypy .

format:
	ruff format .
	ruff check --fix .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf .mypy_cache
	rm -rf .ruff_cache

build:
	docker-compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f aipal-backend

shell:
	docker compose exec aipal-backend /bin/bash

migrate:
	alembic upgrade head

init-db:
	@echo "Initializing database with seed data..."
	python scripts/init_data.py

check: lint test
	@echo "All quality checks passed!"

# Development with live reload
dev-docker:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml up

# Database operations
db-up:
	docker compose up -d postgres redis

db-down:
	docker compose down postgres redis

# Monitoring
monitoring-up:
	docker compose up -d prometheus grafana

monitoring-down:
	docker compose down prometheus grafana
