CREATE TABLE shipment_products (
    shipment_product_uuid STRING NOT NULL REFERENCES shipment (shipment_uuid),
    shipment_uuid STRING NOT NULL,
    product_id STRING NOT NULL,
    product_qty INT NOT NULL,
    meta_source_event_ids STRING,
    meta_source_event_names STRING,
    meta_insert_timestamp TIMESTAMP,
    meta_updated_timestamp TIMESTAMP,
    PRIMARY KEY (shipment_product_uuid)
);
