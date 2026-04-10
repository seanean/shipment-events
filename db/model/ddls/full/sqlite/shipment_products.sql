CREATE TABLE shipment_products (
    shipment_product_uuid TEXT NOT NULL REFERENCES shipment (shipment_uuid),
    shipment_uuid TEXT NOT NULL,
    product_id TEXT NOT NULL,
    product_qty INTEGER NOT NULL,
    meta_source_event_ids TEXT,
    meta_source_event_names TEXT,
    meta_insert_timestamp TEXT,
    meta_updated_timestamp TEXT,
    PRIMARY KEY (shipment_product_uuid)
);
