CREATE TABLE shipment (
    shipment_uuid STRING NOT NULL,
    shipment_business_id STRING NOT NULL,
    shipment_technical_id STRING NOT NULL,
    shipment_type STRING NOT NULL,
    meta_source_latest_event_id STRING NOT NULL,
    meta_source_latest_file_path STRING NOT NULL,
    meta_source_event_id_lst STRING NOT NULL,
    meta_source_file_path_lst STRING NOT NULL,
    meta_root_business_key STRING NOT NULL,
    meta_insert_timestamp TIMESTAMP NOT NULL,
    meta_updated_timestamp TIMESTAMP,
    PRIMARY KEY (shipment_uuid)
);
