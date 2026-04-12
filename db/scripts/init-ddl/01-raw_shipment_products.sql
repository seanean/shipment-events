CREATE TABLE raw.shipment_products (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id VARCHAR NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    event_name VARCHAR NOT NULL,
    payload JSONB NOT NULL,
    meta_insert_timestamp TIMESTAMP
);
ALTER TABLE raw.shipment_products OWNER TO shrw;