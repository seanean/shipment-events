CREATE TABLE cur.shipment_status (
    shipment_status_uuid VARCHAR NOT NULL,
    shipment_uuid VARCHAR NOT NULL REFERENCES cur.shipment (shipment_uuid),
    shipment_status VARCHAR NOT NULL,
    shipment_status_tmst TIMESTAMPTZ NOT NULL,
    meta_source_latest_event_id VARCHAR NOT NULL,
    meta_source_latest_file_path VARCHAR NOT NULL,
    meta_source_event_id_lst VARCHAR NOT NULL,
    meta_source_file_path_lst VARCHAR NOT NULL,
    meta_root_business_key VARCHAR NOT NULL,
    meta_source_tmst TIMESTAMPTZ NOT NULL,
    meta_insert_tmst TIMESTAMPTZ NOT NULL,
    meta_update_tmst TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (shipment_status_uuid)
);
ALTER TABLE cur.shipment_status OWNER TO shrw;
