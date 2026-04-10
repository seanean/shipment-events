.PHONY: setup raw

VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

setup:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

raw:
	@if [ ! -x "$(PYTHON)" ]; then \
		echo "Run 'make setup' first."; exit 1; \
	fi
	$(PYTHON) src/main.py

compose:
	docker compose up -d