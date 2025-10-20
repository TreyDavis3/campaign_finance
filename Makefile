# Simple makefile for common tasks
.PHONY: help migrate etl test

help:
	@echo "make migrate     - Run DB migrations"
	@echo "make etl         - Run full ETL (reads .env)"
	@echo "make test        - Run pytest"

migrate:
	python migrations/upgrade.py

etl:
	python -m src.cli run

test:
	python -m pytest -q
