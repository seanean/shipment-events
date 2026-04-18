CREATE TABLE cln.shipment_status (
    event_id VARCHAR NOT NULL PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    event_name VARCHAR NOT NULL,
    payload_cln JSONB NOT NULL,
    raw_offset_id BIGINT NOT NULL,
    meta_insert_timestamp TIMESTAMP NOT NULL,
    meta_update_timestamp TIMESTAMP NOT NULL,
    meta_source_file_path VARCHAR NOT NULL
);
ALTER TABLE cln.shipment_status OWNER TO shrw;
CREATE INDEX idx_cln_shipment_status_event_id ON cln.shipment_status (event_id);