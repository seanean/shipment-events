# Shipment Events

## Intro

Welcome to my `shipment-events` repo, where I'm working on some data pipelines for, you guessed it, shipment events!

Want to read about the project? Check the [project blog](BLOG.md).

---

## Roadmap

| Phase | What | With | Status |
|-----|-----|-----|-----|
|1|Setup + Raw|Python, Docker|Done|
|2|Raw + Quarantine|Python, Docker|Done|
|3|Cleansed|Python, Docker|Done|
|4|Curated|Python, Docker|Done|
|5|Curated in dbt|dbt, Python, Docker|WIP|
|...|I have an idea, but I'll decide later|...|...|

**My to-do list:**
- `Layered pipelines / medallion architecture`
  - [✔] Raw / [✔] Silver / [] Gold
- `Event-based data ingestion` 
  - ✔ `Local`
  - `Event generator`
  - `Flask REST API + Endpoint`
  - `Kafka`
- ✔`Schema validation`
- ✔`Quarantining`
- ✔`UUID generation`
- ✔`Unit testing`
- `Automated testing`
- `CICD with Github Actions`
- ✔`PostgreSQL`
- ✔`Database access management`
- `dbt`
- `Spark / PySpark`
- ✔`Idempotent pipelines`
- `Checkpoints`
  - ✔ Kinda, using pipeline_run history table.
- `Airflow`
- `Reprocessing`
- `Dead letter queue`
- `Programmatic STM implementation`

---

## Project Documentation

### Layout

Some stuff I thought worth pointing out in the project (not everything):
```
project-structure/
├── Makefile         # venv, docker, pipelines, dbt, tests, type checks
├── config/          # yaml for reusability (preparing for different environments maybe?)
├── db /              
│   └── model        # curated (silver) model
├── landing-zone/    # sample events I created
├── schemas/         # event schemas for the shipment events!
├── src/             # python stuff
├── blog.md          # day to day updates about the project and what I'm learning
└── readme.md        # project docs + phase docs
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
- `make curate` — run curated layer (`src/curate.py`)
- `make increment` — move files from `landing-zone/to-incrementally-move-in/` to `pending/`, then `raw`, `cleanse`, `curate`
- `make run` — `composeup`, then `raw`, then `cleanse`, then `curate` (full stack in order)
- `make rerun` — `resetall`, then same as `run` (clean DB + LZ, then full pipeline)

**Reset local state**

- `make resetall` — tear down Postgres data volume, then run `resetlz` (DB + landing zone)
- `make resetdb` — tear down DB volume only
- `make resetlz` — restore incremental sample files from `landing-zone/archive/` into `landing-zone/to-incrementally-move-in/`, sync `archive/` and `quarantine/` back into `landing-zone/pending/`, then clear archive and quarantine

**dbt** (uses `compose.yaml` + `compose.dbt.yaml`; Postgres should be up, e.g. `make composeup`)

- `make dbtrun` — `dbt run` in the dbt service container
- `make dbtbuild` — `dbt build`
- `make dbtdeps` — `dbt deps`
- `make dbtmakeclnsrc` — `dbt run-operation generate_source` for schema `cln` (column metadata)

**Checks**

- `make test` — `pytest -v`
- `make mypy` — `mypy` on `src`
---

## Version Updates

### Phase 5 - dbt: Cleansed -> Curated 2026-05-12

dbt has been implemented!
- dbt run via docker container
- src ymls generated with codegen
- stg models per source event
- cur models per target table
  - different from python: a single model for shipment, combining data from both events' cln data
- all incremental
  - different from python: no batching, so a full reprocess would be heavy. _imo something to handle via distributed compute or batching, not sure if i'll implement_
- tests added: unique, not null, relationship

my impression: it's a bit odd relinquishing control of SQL / DDL, but I understand the quality of life dbt can bring.

#### Data Flow
<h1 align="center">
  <img src="resources/shipment-events - phase-5-data-flow.png" alt="Shipment Events Phase 5 Data Flow" width="950">
</h1>

#### Overview
<h1 align="center">
  <img src="resources/shipment-events - phase-5.png" alt="Shipment Events Phase 5" width="950">
</h1>

The tricky thing in dbt was getting the shipment entity to work incrementally as it has two separate sources feeding it (aka -> need to track two separate timestamps/offsets). Ultimately, I went for storing both indicators as meta fields. Not the most scalable solution (i.e. imagine having 10 timestamps to track), but in this context it works.


### Phase 4 - Cleansed -> Curated 2026-04-23

Curated is working!
- incremental ingest from cleansed
- orchestrated in python, but full batch processing done within SQL
- batch all occurs within one transaction, so issues are rolled back
- orphans from shipment_product are deleted (functional decision: latest event is correct)
- tracking run history in meta.pipeline_run
- additive traceability in meta fields: see what cln data was merged into a single cln record.

#### Overview
<h1 align="center">
  <img src="resources/shipment-events - phase-4.png" alt="Shipment Events Phase 4" width="950">
</h1>


### Phase 3 - Raw -> Cleansed - 2026-04-19

Cleansed is working!
- incremental ingest from raw
- merging on keys (within batches and on inserts)
- adding uuids
- storing cln
- tracking run history in meta.pipeline_run
- additive traceability in meta fields: see what raw data was merged into a single cln record.

#### Overview
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


#### Overview
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

#### Overview
<h1 align="center">
  <img src="resources/shipment-events - phase-1.png" alt="Shipment Events Phase 1" width="950">
</h1>

(This diagram doesn't follow any standard, it's just a combination of stuff to show generally how this phase works)
