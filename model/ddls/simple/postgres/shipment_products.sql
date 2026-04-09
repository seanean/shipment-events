CREATE TABLE shipment_products (
    shipment_product_uuid VARCHAR(255) NOT NULL,
    shipment_uuid VARCHAR(255) NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    product_qty INTEGER NOT NULL,
    meta_source_event_ids VARCHAR(255),
    meta_source_event_names VARCHAR(255),
    meta_insert_timestamp TIMESTAMP,
    meta_updated_timestamp TIMESTAMP
);
