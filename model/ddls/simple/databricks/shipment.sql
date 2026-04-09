CREATE TABLE shipment (
    shipment_uuid STRING NOT NULL,
    shipment_business_id STRING NOT NULL,
    shipment_technical_id STRING NOT NULL,
    shipment_type STRING NOT NULL,
    meta_source_event_ids STRING,
    meta_source_event_names STRING,
    meta_insert_timestamp TIMESTAMP,
    meta_updated_timestamp TIMESTAMP
);
