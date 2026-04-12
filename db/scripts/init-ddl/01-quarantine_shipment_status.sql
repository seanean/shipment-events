CREATE TABLE quarantine.shipment_status (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id VARCHAR NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    event_name VARCHAR NOT NULL,
    payload JSONB NOT NULL,
    error_message VARCHAR NOT NULL,
    traceback_message VARCHAR NOT NULL,
    meta_insert_timestamp TIMESTAMP NOT NULL,
    meta_source_file_path VARCHAR NOT NULL
);
ALTER TABLE quarantine.shipment_status OWNER TO shrw;