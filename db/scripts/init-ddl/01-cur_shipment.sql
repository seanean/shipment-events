CREATE TABLE cur.shipment (
    shipment_uuid VARCHAR NOT NULL,
    shipment_business_id VARCHAR NOT NULL,
    shipment_technical_id VARCHAR NOT NULL,
    shipment_type VARCHAR NOT NULL,
    meta_source_latest_event_id VARCHAR NOT NULL,
    meta_source_latest_file_path VARCHAR NOT NULL,
    meta_source_event_id_lst VARCHAR NOT NULL,
    meta_source_file_path_lst VARCHAR NOT NULL,
    meta_root_business_key VARCHAR NOT NULL,
    meta_insert_timestamp TIMESTAMPTZ NOT NULL,
    meta_update_timestamp TIMESTAMPTZ,
    PRIMARY KEY (shipment_uuid)
);
ALTER TABLE cur.shipment OWNER TO shrw;
