CREATE TABLE cur.shipment_product (
    shipment_product_uuid VARCHAR NOT NULL,
    shipment_uuid VARCHAR NOT NULL REFERENCES cur.shipment (shipment_uuid),
    product_id VARCHAR NOT NULL,
    product_qty INTEGER NOT NULL,
    meta_source_latest_event_id VARCHAR NOT NULL,
    meta_source_latest_file_path VARCHAR NOT NULL,
    meta_source_event_id_lst VARCHAR NOT NULL,
    meta_source_file_path_lst VARCHAR NOT NULL,
    meta_root_business_key VARCHAR NOT NULL,
    meta_insert_timestamp TIMESTAMPTZ NOT NULL,
    meta_update_timestamp TIMESTAMPTZ,
    PRIMARY KEY (shipment_product_uuid)
);
ALTER TABLE cur.shipment_product OWNER TO shrw;
