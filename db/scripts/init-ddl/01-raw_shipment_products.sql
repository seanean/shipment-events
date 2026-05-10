CREATE TABLE raw.shipment_products (
    offset_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id VARCHAR NOT NULL,
    event_tmst TIMESTAMPTZ NOT NULL,
    event_name VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    payload JSONB NOT NULL,
    meta_insert_tmst TIMESTAMPTZ NOT NULL,
    meta_source_file_path VARCHAR NOT NULL
);
ALTER TABLE raw.shipment_products OWNER TO shrw;
CREATE INDEX idx_raw_shipment_products_offset_id ON raw.shipment_products (offset_id);