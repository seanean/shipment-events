CREATE TABLE shipment_products (
    shipment_product_uuid VARCHAR(255) NOT NULL REFERENCES shipment (shipment_uuid),
    shipment_uuid VARCHAR(255) NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    product_qty INTEGER NOT NULL,
    meta_source_event_ids VARCHAR(255),
    meta_source_event_names VARCHAR(255),
    meta_insert_timestamp TIMESTAMP,
    meta_updated_timestamp TIMESTAMP,
    PRIMARY KEY (shipment_product_uuid)
);
