.PHONY: setup raw

setup:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt

raw: # check if python exists and is executable
	@if [ ! -x ".venv/bin/python" ]; then \
		echo "Run 'make setup' first."; exit 1; \
	fi
	.venv/bin/python src/main.py

composeup:
	docker compose up -d

composedown:
	docker compose down

resetall: # clean up the db and reset LZ
	docker compose down && docker volume rm shipment-events_postgres_data
	mv landing-zone/archive/* landing-zone/pending/

resetdb: # clean up the db
	docker compose down && docker volume rm shipment-events_postgres_data

resetz: # reset LZ
	mv landing-zone/archive/* landing-zone/pending/