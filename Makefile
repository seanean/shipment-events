.PHONY: setup raw test

setup:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt

setup-dev:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements-dev.txt

raw: # check if python exists and is executable
	@if [ ! -x ".venv/bin/python" ]; then \
		echo "Run 'make setup' first."; exit 1; \
	fi
	.venv/bin/python src/ingest_raw.py

cleanse: # check if python exists and is executable
	@if [ ! -x ".venv/bin/python" ]; then \
		echo "Run 'make setup' first."; exit 1; \
	fi
	.venv/bin/python src/cleanse.py

composeup:
	docker compose up -d --wait
	@echo "PostgreSQL is ready to use."

composedown:
	docker compose down

resetall: # clean up the db and reset LZ
	docker compose down && docker volume rm shipment-events_postgres_data
	rsync -a landing-zone/archive/ landing-zone/pending/
	rm -rf landing-zone/archive/*
	rsync -a landing-zone/quarantine/ landing-zone/pending/
	rm -rf landing-zone/quarantine/*

resetdb: # clean up the db
	docker compose down && docker volume rm shipment-events_postgres_data

resetlz: # reset LZ
	rsync -a landing-zone/archive/ landing-zone/pending/
	rm -rf landing-zone/archive/*
	rsync -a landing-zone/quarantine/ landing-zone/pending/
	rm -rf landing-zone/quarantine/*

test:
	@if [ ! -x ".venv/bin/python" ]; then \
		echo "Run 'make setup' first."; exit 1; \
	fi
	.venv/bin/pytest -v

mypy-raw:
	.venv/bin/mypy src/ingest_raw.py

mypy-cleanse:
	.venv/bin/mypy src/cleanse.py

rerun:
	make resetall && make composeup && make raw && make cleanse