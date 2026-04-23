# shipment-events Blog

## Blog...?

Yeah. I'm going to write updates about how the project goes.

## What is this project?

Hi!

I started this project as a way to get more hands-on experience with the tools and concepts that my engineering colleagues have been using to develop the data products I have been analyzing and designing. 

In the readme I think I'll give updates on each phase of the project, but for now here are _some_ highlights of concepts I'm planning to implement as part of my roadmap for `shipment-events`:
- `Layered pipelines / medallion architecture`
- `Event-based data ingestion`
- `REST APIs`
- `Kafka`
- `Schema validation`
- `Quarantining`
- `UUID generation`
- `Unit testing`
- `Automated testing`
- `CICD with Github Actions`
- `PostgreSQL`
- `Database access management`
- `dbt`
- `Spark / PySpark`
- `Idempotent pipelines`
- `Checkpoints`
- `Airflow`

### Expanded list of things I thought about and don't want to forget

- `Reprocessing`
- `Dead letter queue`
- `Programmatic STM implementation`

### Known weaknesses

- `No concurrency` - currently, I haven't done anything to allow multiple versions of the pipeline to run at the same time. There's some cleanup logic that could impact continuous loading / removing of data. My guess: look at this when piping tons of events in or maybe when I try PySpark?

### Don't forget

- integration tests! (for later)
- Apply raw schema to make querying from cleansed to curated consistent (i.e. provide null values when attributes aren't present.) ??
- add indexes on the meta_root_business_key on all tables

## How am I developing it?

I'm mostly writing things by hand. I'm doing a lot of googling, watching videos, and reading documentation. I'm also using a lot of AI, but **I am not vibe coding this pipeline**.

I know I could just say "ingest json, do validation, do this, do that" (like I have for various other tools I've made) and probably vibe code this thing in a day, but **I'm trying to actually learn this stuff** so I'm not doing that.

How I am using AI:
- helping to figure out a roadmap
- a LOT of questions and discussions
- some Cursor tabbing _(within reason, trying to avoid learning nothing)_
- some agenting  _(docs, some changes where I know exactly what to do & how)_

---

## Phase 1 - Setup & Raw

### Did

#### The plan
- Came up with a plan for how to iteratively build this thing and learn these concepts.
- Drafted a use case for data to process; events about shipments that could conceivably exist in any logistics organization. Two types: (1) statuses a shipment has reached and (2) product breakdown of a shipment. 
- Designed event schemas and a data model using my [Madalier](https://github.com/seanean/madalier) tool (I made it for me, you can use it if you want). 
- Did some research on some topics that are in the plan.
- Figured out how I want to tackle blog vs readme.

#### Setup
- Set up the initial scaffold of this project.
- Added more make commands to reset the environment: `make resetall` `make resetdb` `make resetlz`
- Renamed compose make commands to `make composeup` and `make composedown` as that just sounds better to me than dbup/dbdown.

#### General
- A lot of phase 1 work
- Cleanup of the blog
- Added a LZ pending and LZ archive setup
- Automatically cleaning LZ pending after a file is loaded successfully
- Cleaning up empty LZ pending folders after raw ingest
- Added better logging.

### Learned

| Topic | Learning |
| --- | --- |
| Github Actions / CICD | What I'll use GitHub Actions for (and what that costs). How I won't actually be doing the CD of CICD because I'm working locally (maybe later if I get to cloud deployments). |
| [The Twelve Factor App](https://12factor.net/) | There are a LOT of books, sites, blogs, etc. one can read about how to be better at development. 2017 was a while ago, so some guidelines are likely less relevant today, but I think some will teach me about some pitfalls to avoid. Adding to pin this for later. |
| Credential management | Looked at how I'd do this in line with factor 3 in earlier phases (.env) and later phases (credential vault of some form?) |
| Docker — image selection | Also came across this in my music app. Here I chose the latest stable Debian (Trixie) over Alpine to avoid issues with Python. If nothing I end up using conflicts with Alpine I could switch later. |
| Docker — health checks | Know when things are ready, makes sense. |
| here-docs | Sending bigger text with bash commands, specifically did this in the context of commands to run on container start when initing the DB.|
| PostgreSQL sequences | Sequences being distinct things + Serial being old + Identity being a bit more robust. |
| PostgreSQL role management | came up with my strategy:<br><ul><li>`PG default user` sets up initial schemas and manages permissions</li><li>`shrw` user can create and read everything (except `public`), simulates an engineering team.</li><li>`shr` user can read everything (except `public`), simulates a role used for like Tableau or something.</li><li>`shcon` user can read `cur`, simulates a business user.</li><li>Later on if I make `cons` schema, I'll have to update accesses. Probably same setup as cur for shcon. I could, for example, make an `shanalyst` role which can rw in a `cons` only.</li><li>I also got to test doing the things DB admins have been doing for me at work (granting usage, default privileges, etc.)</li></ul> |
| configuring paths | Something I liked was how we use yamls to configure things across environments for deploys. So I'm going to do this even though I have one environment! hello `config/dev.yaml`. Also using `.env` for credentials. |
| partioning conventions | looked at some options, some of which I have seen before. i'm going to partition by event and dt, so `landing-zone/event_name/dt=YYYY-MM-DD`. Another common pattern is `year=2025/month=10/day=15/hour=11` _yes partioning doesn't matter when I have 4 events, but I might generate a ton in the future, so I figured why not just already take this into account?_ |
| balancing functions vs inline code | this is always something I've been curious about. I often write stuff in-line then eventually turn it into functions later on when I realize I need to duplicate logic. In this project, I'm trying to do a bit more thinking upfront about the flow. |
| `__init__.py` and structuring code for airflow | after a lot of investigation, it's looking like a good way to eventually setup logic for airflow is to call modules within the project with args like `python -m src.ingest_raw --data shipment_status` for raw and others for later steps. </br> </br> to begin, I won't do that, I'll put the logic together in `main.py` so i can import functions from the other files and sequence it there all as one. however, I'll still setup `__main__` blocks and `__init__.py` to support working with Airflow in this way in the future. |
| SQLAlchemy | seems quite simple to work with. I still haven't mastered the concepts of connection pooling and managing sessions. For now I got querying working with the engine as a singleton. Later I'll look more into optimizing the pattern.|
| sequencing permissions for tables | I have a bit of a funky order for this because I'm creating this on DB initialization. Normally an engineering team wouldn't be creating tables on DB initialization with root users, they'd make their own tables. As I am, I need to change the ownership to the engineering role to pretend that they made it. This allows user `shrw` to insert / delete / whatever, once the database is running. What this means for me is that my pattern in this project isn't exactly 'real', but that's ok I suppose.|
| Exception handling | I spent a lot of time looking at the different ways to handle (or not handle) exceptions. It's still not clear to me what the best approach is. I feel I will need balance between try catching everything in existence vs nothing. I think it will also pair with how I approach logging. Definitely something REQUIRED for when I get to a more mature 'this is a pipeline' phase, but for now I'll see how much I do. | 
|Avoiding multiple raw loads|I considered different approaches: 1) moving files after processing. 2) maintaining a log of files which were processed. 3) relying on unique inserts to avoid double loading. 4). trying to replicate databricks checkpoint logic. 5) tracking processed partitions. </br> </br> what I landed on was option 1) as it seems quite simple, and I think for reprocessing we can just add logic to load from the archive folder rather than the pending folder (to avoid having to move files back).|


## Phase 2 - Quarantine, Testing, Typing

### Did

#### quarantining
- Decided on approach to follow for payloads that don't conform to schema:
```
    validate vs schema

    if fails
        write file, error, metadata to quarantine.invalid_events
        move file to lz/quarantine
        if move file fails
            rollback insert
        go to next file

    continue normal logic
```
- created quarantine tables
- created some bad events for validating the validating (the validating the validating)
- added a rollback on the insert in case file move to archive/quarantine fails
#### table tweaks
- reordered raw table columns, also changed varchar(x) to varchar (not recommended to use specified length in Postgres)
- added meta_insert_timestamp to raw and quarantine so you can see when records are added.
- added meta_source_file_path so I can see where files came from

#### refactoring
- consolidated insert / row building / file movement logic to functions that can be used for both archiving and quarantining payloads
- reworked paths so they are based on the repo root instead of relative. No longer constructing any paths based on user input (yikes).
- reworked yaml x string transforms x functions to centralize / reduce transformations and number of variables going around
    - i.e. i added *_insert_sql_path instead of creating it by combining strings.

#### database access
- fixed db access pattern/setup. problem: superuser ran creates, default privileges not granted. solution: `02-rw-run-ddl.sh` now logs in as shrw and runs all DDLs, which gives shr and shcon access to query.


#### testing & typing
- more tests! + refactoring to make testing easier/possible.
- Type hinting! I knew this was a thing, but hadn't done it yet.
- Reworked the resolved config to be a dataclass for better type hinting capabilities. MyPy was not happy about a string dict['value'] being passed when dict was dict[str | Path]. Now the data class has types so I can pass dict.value.

### Other thoughts

Things I wanted/want to do before cleansed:
- ✓ testing (mostly done, won't go to integration testing in this phase)
- ✓ typing
- ✓ quarantining
- ✓ aggregate logging

I don't really like how I'm doing my `insert_row_builder`s. I'll probably think of another way to do it eventually. some thoughts on that:
- my model also generates STMs (event property->model attribute). Should ingest to reduce duplication of logic.
- prereqs: 1. would need to connect (model stm / payload structure) -> (naming in code). 2. need to add raw/quarantine to model.
- if possible, then I can generate maybe generate this mapping logic by loading in the model stm csv
- naming in code -> need to solve how `file["event_timestamp"]` != `payload.event_timestamp` (or however i'd represent root level event info)

I should probably add some aggregate logging. (ingested x files, archived x, quarantined x, start partition, end partition, etc.)

Experimenting with caveman mode to see if I can use less tokens and learn faster :)


### Learned

| Topic | Learning |
| ----- | -------- |
| Testing | Didn't get much time to work on everything today, but I did spend some time working on more tests. It's becoming clear to me what types of patterns are more conducive to testing and what are not. </br> </br> Working with tmp_path to facilitate tests is cool. I'm at the point now where I think everything needs integration tests so I'll probably dive into that next. | 
| Type Hinting | There's a bit of finesse to it in finding the sweet spot between _oh god! : exclaimation I have typed everything into existence : str_ and _I don't know what I'm looking at_. I definitely would not claim I know the perfect way to do it, but I did some investigations into alternative approaches. Like testing, it also shapes how you code. |
|Dead letter queues| Can vary in terms of how they are implemented, but generally the point of a DLQ is to track payloads that have problems, generally so you can do something about it. </br> </br> Here are some options at varying levels of complexity:<ul><li>Try catching validation, logging problem files & skipping, go to next file.</li><li>Try catching validation, logging problem files, storing files in a folder, storing info in a db schema, skipping, go to next file.</li><li>Try catching validation, logging problem files, posting problem files on team's own DLQ topic that team can use for replay processing</li><li>Maybe adding some reporting on errors.</li></ul> |
|rsync instead of mv| mv doesn't work well if there are files existing in the folder we're copying to. in the makefile I have make resetall / make resetlz which moves stuff from the archive back to the landing-zone. I swapped it out with rsync + rm -rf to allow for merging of archive back to the LZ. this does mean that if the same filename is in archive and in LZ, the LZ would be overwritten. This is more of a dev tool so for me that's fine. |
| compose up --wait | beautiful, I can use my health check to know when things are good to go. (I kept hitting errors by `make raw`ing too soon.) |
|bash|I get why there's a running joke about everything being based on a bash script and a cron job. Seems like such a flexible tool. Happy with the reworked init.|

## Phase 3 - Cleansed

### Some Thoughts
Things I'd like to do in cleansed:
- ✓ Incrementally retrieve/process/load data
    - ✓ pipeline history table
    - ✓ batch retrieval based on offset_id
    - ✓ cleansing in python
    - ✓ merging within batch in python (prevent double keys in sql insert)
    - ✓ batch insert (incl. merge) via executemany
    - ✓ handle empty runs
    - ✓ log aggregates
- ✓ Update raw DDLs (add indexes for batching)
- ✓ Create cleansed DDLs (w/ event id index)
- ✓ Create pipeline history DDL
- ✓ Merge on event ID
- ✓ Generate UUIDs
- ~~Cast to data types~~ wasn't needed.
- ✓ Drop unneeded fields
- For later: Add default values
    - probably should map events to raw schema so that missing optional fields are created with default values?
- ✓ Log aggregates

Researched:
- how do I handle merging with incremental stuff?
- how do I not have to load everything in memory?

### Did

- Reworked make and logging to allow triggering of raw or cleansed directly.
- set up cleansed tables
- set up a pipeline run table to store info on cleansed runs (probably other layers as well) and keep track of how many IDs have been processed
- selected from raw
- deduplicated on latest offset_id per event id
- added uuids to the different events
- centralized get_insert and insert_row_builder and added support for cln tables
- inserting records for cln
- inserting records into pipeline_run on success or failure
- looping through batches
- passing around a million variables was not very fun so i made a dataclass. much nicer experience.
- testing added
- made logging a little prettier
- reworked get_engine so it's called and passed to support unit testing

### Learned


| Topic | Learning |
| --- | --- |
| Incremental Processing - detecting progress | A couple of ways to do this. 1. `storing progress on raw` - this would mean having a `processed_at` field on raw table. I don't like this because it means altering raw records a lot.  |
| Incremental Processing - detecting progress |2. `pipeline state table` - something like 'latest from raw.t1 is yesterday'. I like this. |
| Incremental Processing - detecting progress |Dug in a lot on the topic of how to handle batching. I'm going to use a `pipeline_run_history` table instead of a `pipeline_state_table`, but it will serve a dual purpose. a key question for me was how to track the progress of cleansed. If you only insert on completion and run batches of 200 and need to process 1000 in total, if you fail at 500 then you don't have a `success` in the `state`/`history` table and your next run will redo the first 499. To circumvent this somewhat, I will add run+batch records in the `history` table. It's possible that a run is incomplete, but the next run will pick up from the last completed batch. If a run fails, will insert a run record with status failed for historical tracking. |
| Fetch Strategy | 1. a [`server-side cursor`](https://www.postgresql.org/docs/current/plpgsql-cursors.html) - `Rather than executing a whole query at once, it is possible to set up a cursor that encapsulates the query, and then read the query result a few rows at a time. One reason for doing this is to avoid memory overrun when the result contains a large number of rows.` It makes sense, but there are details about behavior of the connection / transaction that I'd have to figure out. Might prefer option 2.|
| Fetch Strategy | 2. `batch querying` - linked to pipeline state. Can batch by a date or by a row ID. can fine-tune batching to the size of the data which is nice (wide table 5 rows, narrow table 50 rows).  |
| Event merging | To some degree you can rely on Postgres' merge on key logic. However, if you're going to bulk insert (like I will be) you'll need to avoid that a single batch contains >1 of the same event ID. Strategy will be a combination of both: merge in batch in Pandas, merge in table in DB. </br> </br> Another alternative could be to have a post-cleansing staging table that has everything, then I select a `distinct on event ID order by event timestamp desc` into the actual cleansing table. |
| Unit testing DB functions | By passing in their engine dependency, I can test (some) DB functions in unit tests! sqlite yay. |


## Phase 4 - Curated

### Some thoughts

It's time! We're almost there!!!

First, some upgrades to phase 3 to give better traceability across layers:
- changed a bunch of things but i landed on:
    - `meta_source_latest_event_id` - what's the latest event that populated this record
    - `meta_source_latest_file_path` - what's the latest file that populated this record
    - `meta_source_event_id_lst` - which events fed into this record
    - `meta_source_file_path_lst` - which files fed into this record
    - `meta_root_business_key` - useful for orphans or delete and replace logic
- also updated my model (and Madalier itself)
    - better data type coverage
    - fixing some bugs with meta fields

Other:
- small bug in cleansed fixed to give iterative started_at dates

Phase 4 stuff:
- as always, some things needed to be refactored from cleansed. I limited how much I refactored because next phase will involve DBT replacing curated.
- config work to accomodate curated
- general flow for curated:
    1. get config, resolve it
    2. detect where to start (based on cln)
    3. trigger batch sql*
    4. record success
    5. detect if we need to keep going
    6. if issues, record failure
- with a batch flow of*
    1. select batch of new cln data and store in temp table
    2. transform that into temp tables for each target table
    3. use temp tables to upsert remaining records
    4. detect orphans in target tables, delete them
    5. update pipeline run table with new latest offset

It took a lot of time to land on how I wanted to do curated because of endlessly changing my mind on meta fields and trying to decide on how I wanted to handle upserting / replacing / orphans. Ultimately I went with a SCD1 flow (latest event is the truth) + some visibility on the past via meta fields.

### Learnings

| Topic | Learning |
| --- | --- |
| SQL-based curated processing | Cleansed logic was handled with data frames and inserting results (and handling conflicts on insert). Curated will be different (SQL) and more complicated (more complex than just merging on event ID). The processing will very much depend on what type of end result I want. </br> </br> What I'm aiming for is mixed depending on the event. Shipment header should reflect the latest version received. Statuses should show all. Products should show the latest set received (in case of >1 event with same business key). This means I'll need orphan handling. My additive traceability requirements also have to be meshed with these rules. The general approach I will use is to load batches in temp tables to detect & handle orphans and then upsert data.|
| Additive traceability | Had to give some thought to how exactly I want to handle this. In cleansed I have meta_source_file_path_lst and meta_raw_offset_id_lst which tell me what raw records ended up in cleansed.</br></br> For curated, I want any upserts to also show this history, but I can't ONLY upsert because then I would end up with orphans that shouldn't be present. |
| Orphan Handling | Looked at different ways to do this. There are trade-offs in terms of how much searching you have to do to find and drop orphans. Ultimately I decided on: 1) add parent key to all entities 2) when loading a batch, filter target tables on the new parent keys (i.e. shipment_product) and then compare the child keys vs the new child keys. 3) delete any current records with no matching key in the new records 4) upsert the remaining records. </br> </br> Something I discussed with Brend, if you have separate transactions then deleting should FOLLOW upserting because if something goes wrong you might end up removing data without being able to add its replacement. I'm doing all of it in one transaction so I don't have to worry, but good to say. |
| Partioning, indexing, clustering | I knew partioning, like indexing, can have impacts on how easily you can scan data and how filters perform. This was relevant for the orphan handling. I'll likely test partioning on parent_key (vs not) when loading lots of data to see how it impacts performance. I also discussed clustering with Brend and gained a better understanding of how it is important for distributed computing, specifically that by clustering data properly you can distribute the compute broadly and still have the correct outcomes. For example, if event 1-2-3 should touch the same record in the end, you'll want to cluster them together, but event 7 can be in a separate cluster as it has nothing to do with the other 3. </br></br></br> _AI Note I generated for clarification: “partitioning” and “indexing” here are about physical data layout and lookup; “clustering” in distributed systems often means grouping related keys so the same record/work lands together (e.g. shuffle / co-partitioning), which is a different idea than e.g. a warehouse “cluster key” for pruning—same word, two families of meaning._ |
|Transactions for batch processing|One of the benefits of using SQL to go from CLN -> CUR is that I can do all of my statements for a batch within one transaction, so if something fails it will fully roll back that batch. I also won't have to worry about temp tables disappearing. (I know they don't disappear after a transaction finishes, but how SQLAlchemy engine pooling works vs connections is not 100% clear and it seems like it would not be crazy to get fed a different connection. |
|Tracking batches in SQL|One question I had was how best to detect whether there's more data to process or not. In CLN, I loaded the next batch into a DF and if the length was 0, I knew I was done. In SQL, I'm not specifically selecting anything into my python script. A solution I'm going for is actually to just do that. (a select count(1) or exists select 1 where next batch)|
|Handling failed batches|If an insert of a batch to cur fails, my transaction won't continue to the `insert pipeline run history` step, so I'll have to detect if it failed and insert that failure manually. Because I'm using SQLAlchemy with psycopg, it raises its own exceptions (i.e. sqlalchemy.exc.SQLAlchemyError) instead of feeding the [psycopg errors](https://www.psycopg.org/psycopg3/docs/api/errors.html#db-api-exceptions) directly. So, I need to catch the sqlalchemy exceptions (or just exception in general), if I want to detect when batches fail. If caught, then I can trigger the pipeline failed insert. | 