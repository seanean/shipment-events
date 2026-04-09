CREATE TABLE shipment_status (
    shipment_status_uuid VARCHAR(255) NOT NULL REFERENCES shipment (shipment_uuid),
    shipment_uuid VARCHAR(255) NOT NULL,
    shipment_status VARCHAR(255) NOT NULL,
    shipment_status_tmst TIMESTAMP_NTZ NOT NULL,
    meta_source_event_ids VARCHAR(255),
    meta_source_event_names VARCHAR(255),
    meta_insert_timestamp TIMESTAMP_NTZ,
    meta_updated_timestamp TIMESTAMP_NTZ,
    PRIMARY KEY (shipment_status_uuid)
);
