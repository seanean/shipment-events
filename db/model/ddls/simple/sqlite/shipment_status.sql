CREATE TABLE shipment_status (
    shipment_status_uuid TEXT NOT NULL,
    shipment_uuid TEXT NOT NULL,
    shipment_status TEXT NOT NULL,
    shipment_status_tmst TEXT NOT NULL,
    meta_source_event_ids TEXT,
    meta_source_event_names TEXT,
    meta_insert_timestamp TEXT,
    meta_updated_timestamp TEXT
);
