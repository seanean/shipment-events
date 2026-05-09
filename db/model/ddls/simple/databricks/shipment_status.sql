CREATE TABLE shipment_status (
    shipment_status_uuid STRING NOT NULL,
    shipment_uuid STRING NOT NULL,
    shipment_status STRING NOT NULL,
    shipment_status_tmst TIMESTAMP NOT NULL,
    meta_source_latest_file_path STRING NOT NULL,
    meta_source_event_type_lst STRING NOT NULL,
    meta_source_file_path_lst STRING NOT NULL,
    meta_root_business_key STRING NOT NULL,
    meta_source_tmst TIMESTAMP NOT NULL,
    meta_insert_tmst TIMESTAMP NOT NULL,
    meta_updated_tmst TIMESTAMP NOT NULL
);
