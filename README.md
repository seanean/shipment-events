# Shipment Events

## Intro

Welcome to my `shipment-events` repo, where I'm working on some data pipelines for, you guessed it, shipment events!

Want to read about the project? Check the [project blog](BLOG.md).

---

## Project Documentation

### Layout

Some stuff I thought worth pointing out in the project (not everything):
```
project-structure/
├── Makefile         # venv, docker, raw/cleanse pipelines, tests, type checks
├── config/          # yaml for reusability (preparing for different environments maybe?)
├── db /              
│   └── model        # curated (silver) model
├── landing-zone/    # sample events I created
├── learnings/       # a folder of all of my notes I'm taking as I go through this
├── schemas/         # event schemas for the shipment events!
├── src/             # python stuff
├── blog.md          # day to day updates about the project and what I'm learning
└── readme.md        # project docs + I'm thinking docs of each release to show its evolution
```

### Running this bad boy

**Setup**

- `make setup` — create `.venv` and install `requirements.txt`
- `make setup-dev` — same as `setup`, but install `requirements-dev.txt` (for tests, mypy, etc.)
- `make composeup` — spin up that postgres db!!!
- `make composedown` — spin down that postgres db!!!

**Pipelines**

- `make raw` — run raw ingestion (`src/ingest_raw.py`)
- `make cleanse` — run cleanse pipeline (`src/cleanse.py`)
- `make run` — `composeup`, then `raw`, then `cleanse` (full stack in order)
- `make rerun` — `resetall`, then same as `run` (clean DB + LZ, then full pipeline)

**Reset local state**

- `make resetall` — tear down DB volume, move archive and quarantine back into `landing-zone/pending`
- `make resetdb` — tear down DB volume only
- `make resetlz` — move archive and quarantine back into `landing-zone/pending`

**Checks**

- `make test` — `pytest -v`
- `make mypy-raw` — `mypy` on `src/ingest_raw.py`
- `make mypy-cleanse` — `mypy` on `src/cleanse.py`
---

## Version Updates

### Phase 3 - Raw -> Cleansed - 2026-04-19

Cleansed is working!
- incremental ingest from raw
- merging on keys (within batches and on inserts)
- adding uuids
- storing cln
- tracking run history in meta.pipeline_run

<h1 align="center">
  <img src="resources/shipment-events - phase-3.png" alt="Shipment Events Phase 3" width="950">
</h1>


### Phase 2 - LZ -> Quarantine - 2026-04-12

Now that's what I call quarantining.

What I did:
- added DDLs + inserts + logic to support quarantining events that fail the schema validation
- reworked access setup have tables be created by engineering role
- consolidated some logic that is used by both good and bad events (getting queries, inserting to db, moving files)

Evening Update:
- Added pytest and some unit tests, many more to go!

2026-04-13 Update:
- More tests and refactoring!


<h1 align="center">
  <img src="resources/shipment-events - phase-2.png" alt="Shipment Events Phase 2" width="950">
</h1>


### Phase 1 - LZ -> Raw - 2026-04-12 (night)

I've got a first version raw working!

What I did:
- design event schemas
- create dummy events
- spin up postgres in docker
- create raw ddls
- set up postgres schemas/table
- set up postgres users & permissions for admin, rw, r, consumer
- setup raw ingestion flow pictured below (using rw role)

<h1 align="center">
  <img src="resources/shipment-events - phase-1.png" alt="Shipment Events Phase 1" width="950">
</h1>

(This diagram doesn't follow any standard, it's just a combination of stuff to show generally how this phase works)
