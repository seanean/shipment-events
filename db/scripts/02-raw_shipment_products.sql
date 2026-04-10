CREATE TABLE raw.shipment_products (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payload JSONB NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    event_name VARCHAR(255) NOT NULL
);
ALTER TABLE raw.shipment_products OWNER TO shrw;