CREATE TABLE quarantine.shipment_products (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id VARCHAR NOT NULL,
    event_tmst TIMESTAMPTZ NOT NULL,
    event_name VARCHAR NOT NULL,
    payload JSONB NOT NULL,
    error_message VARCHAR NOT NULL,
    traceback_message VARCHAR NOT NULL,
    meta_insert_tmst TIMESTAMPTZ NOT NULL,
    meta_source_file_path VARCHAR NOT NULL
);
ALTER TABLE quarantine.shipment_products OWNER TO shrw;