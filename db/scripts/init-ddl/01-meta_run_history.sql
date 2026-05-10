CREATE TABLE meta.pipeline_run (
    history_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id BIGINT NOT NULL,
    batch_id BIGINT NOT NULL,
    job_name VARCHAR NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('success', 'failed')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    starting_from_id_exclusive BIGINT NOT NULL,
    from_id_exclusive BIGINT NOT NULL,
    to_id_inclusive BIGINT,
    rows_read BIGINT,
    rows_written BIGINT,
    error_message VARCHAR,
    traceback_message VARCHAR,
    meta_insert_tmst TIMESTAMPTZ
);

ALTER TABLE meta.pipeline_run OWNER TO shrw;