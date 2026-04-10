CREATE TABLE cur.shipment (
    shipment_uuid VARCHAR(255) NOT NULL,
    shipment_business_id VARCHAR(255) NOT NULL,
    shipment_technical_id VARCHAR(255) NOT NULL,
    shipment_type VARCHAR(255) NOT NULL,
    meta_source_event_ids VARCHAR(255),
    meta_source_event_names VARCHAR(255),
    meta_insert_timestamp TIMESTAMP,
    meta_updated_timestamp TIMESTAMP,
    PRIMARY KEY (shipment_uuid)
);
