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
├── Makefile         # for now, install dependencies, compose containers, run pipelines 
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

- `make setup` - install dependencies
- `make composeup` - spin up that postgres db!!!
- `make composedown` - spin down that postgres db!!!
- `make raw` - ingest files to the raw tables
- `make resetall` - compose down, remove volume, move files back to LZ/pending
- `make resetdb` - compose down, remove volume
- `make resetlz` - move files back to LZ/pending
- `make test` - run unit tests
---

## Version Updates

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
