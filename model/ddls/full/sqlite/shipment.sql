CREATE TABLE shipment (
    shipment_uuid TEXT NOT NULL,
    shipment_business_id TEXT NOT NULL,
    shipment_technical_id TEXT NOT NULL,
    shipment_type TEXT NOT NULL,
    meta_source_event_ids TEXT,
    meta_source_event_names TEXT,
    meta_insert_timestamp TEXT,
    meta_updated_timestamp TEXT,
    PRIMARY KEY (shipment_uuid)
);
