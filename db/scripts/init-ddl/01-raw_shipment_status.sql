CREATE TABLE raw.shipment_status (
    offset_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id VARCHAR NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    event_name VARCHAR NOT NULL,
    payload JSONB NOT NULL,
    meta_insert_timestamp TIMESTAMP NOT NULL,
    meta_source_file_path VARCHAR NOT NULL
);
ALTER TABLE raw.shipment_status OWNER TO shrw;
CREATE INDEX idx_raw_shipment_status_offset_id ON raw.shipment_status (offset_id);