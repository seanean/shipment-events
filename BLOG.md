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

### To Do

- everything above
- add typing wherever possible
- integration tests!

## How am I developing it?

I'm mostly writing things by hand. I'm doing a lot of googling, watching videos, and reading documentation. I'm also using a lot of AI, but **I am not vibe coding this pipeline**.

I know I could just say "ingest json, do validation, do this, do that" (like I have for various other tools I've made) and probably vibe code this thing in a day, but **I'm trying to actually learn this stuff** so I'm not doing that.

How I am using AI:
- helping to figure out a roadmap
- a LOT of questions and discussions
- some Cursor tabbing _(within reason, trying to avoid learning nothing)_
- some agenting  _(docs, some changes where I know exactly what to do & how)_

---

## Phase 1

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


## Phase 2

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
- aggregate logging

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