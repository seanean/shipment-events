CREATE TABLE cur.shipment_product (
    shipment_product_uuid VARCHAR NOT NULL REFERENCES cur.shipment (shipment_uuid),
    shipment_uuid VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    product_qty INTEGER NOT NULL,
    meta_source_event_ids VARCHAR,
    meta_source_event_names VARCHAR,
    meta_insert_timestamp TIMESTAMPTZ,
    meta_updated_timestamp TIMESTAMPTZ,
    PRIMARY KEY (shipment_product_uuid)
);
ALTER TABLE cur.shipment_product OWNER TO shrw;
