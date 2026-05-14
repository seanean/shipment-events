.PHONY: setup raw test increment resetlz resetall restore-incremental-lz generate

# Copied to archive by `make increment`; on reset, moved back from archive before archive→pending.
INCREMENTAL_LZ_FILES := \
	shipment_products/dt=2026-04-05/products_example_e7_sh3.json \
	shipment_products/dt=2026-04-05/products_example_e6_sh2.json \
	shipment_products/dt=2026-04-05/products_example_e8_sh6.json

restore-incremental-lz:
	@for p in $(INCREMENTAL_LZ_FILES); do \
		if [ -f "landing-zone/archive/$$p" ]; then \
			mkdir -p "landing-zone/to-incrementally-move-in/$$(dirname $$p)"; \
			rsync -a --remove-source-files "landing-zone/archive/$$p" "landing-zone/to-incrementally-move-in/$$(dirname $$p)/"; \
		fi; \
	done

increment:
	rsync -a --remove-source-files landing-zone/to-incrementally-move-in/ landing-zone/pending/
	make raw && make cleanse && make curate

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

generate:
	@if [ ! -x ".venv/bin/python" ]; then \
		echo "Run 'make setup' first."; exit 1; \
	fi
	.venv/bin/python src/event_gen.py

cleanse: # check if python exists and is executable
	@if [ ! -x ".venv/bin/python" ]; then \
		echo "Run 'make setup' first."; exit 1; \
	fi
	.venv/bin/python src/cleanse.py

curate: # check if python exists and is executable
	@if [ ! -x ".venv/bin/python" ]; then \
		echo "Run 'make setup' first."; exit 1; \
	fi
	.venv/bin/python src/curate.py

composeup:
	docker compose up -d --wait
	@echo "PostgreSQL is ready to use."

composedown:
	docker compose down

resetall: # clean up the db and reset LZ
	docker compose down && docker volume rm shipment-events_postgres_data
	$(MAKE) resetlz

resetdb: # clean up the db
	docker compose down && docker volume rm shipment-events_postgres_data

resetlz: restore-incremental-lz # reset LZ
	rsync -a landing-zone/archive/ landing-zone/pending/
	rm -rf landing-zone/archive/*
	rsync -a landing-zone/quarantine/ landing-zone/pending/
	rm -rf landing-zone/quarantine/*
	find landing-zone -type f -name '000*' -delete

test:
	@if [ ! -x ".venv/bin/python" ]; then \
		echo "Run 'make setup' first."; exit 1; \
	fi
	.venv/bin/pytest -v

mypy:
	.venv/bin/mypy src

run:
	make composeup && make raw && make cleanse && make curate

rerun:
	make resetall && make composeup && make raw && make cleanse && make curate

# Runner UID ≠ compose user 1000:1000; override on GitHub Actions so /usr/app bind mount is writable.
ifdef GITHUB_ACTIONS
DBT_DOCKER_USER := --user "$(shell id -u):$(shell id -g)"
else
DBT_DOCKER_USER :=
endif

dbtrun:
	docker compose -f compose.yaml -f compose.dbt.yaml run --rm $(DBT_DOCKER_USER) dbt-svc run

dbtbuild:
	docker compose -f compose.yaml -f compose.dbt.yaml run --rm $(DBT_DOCKER_USER) dbt-svc build

dbtdeps:
	docker compose -f compose.yaml -f compose.dbt.yaml run --rm $(DBT_DOCKER_USER) dbt-svc deps

dbtmakeclnsrc:
	docker compose -f compose.yaml -f compose.dbt.yaml run --rm $(DBT_DOCKER_USER) dbt-svc run-operation generate_source --args '{"schema_name": "cln", "generate_columns": true}'

dbtdocgen:
	docker compose -f compose.yaml -f compose.dbt.yaml run --rm $(DBT_DOCKER_USER) dbt-svc docs generate

dbtdocserve:
	docker compose -f compose.yaml -f compose.dbt.yaml run --rm $(DBT_DOCKER_USER) --service-ports dbt-svc docs serve --host 0.0.0.0 --port 8080
