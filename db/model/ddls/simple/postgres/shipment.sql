CREATE TABLE shipment (
    shipment_uuid TEXT NOT NULL,
    shipment_business_id TEXT NOT NULL,
    shipment_technical_id TEXT NOT NULL,
    shipment_type TEXT NOT NULL,
    meta_source_latest_file_path TEXT NOT NULL,
    meta_source_event_type_lst TEXT NOT NULL,
    meta_source_file_path_lst TEXT NOT NULL,
    meta_root_business_key TEXT NOT NULL,
    meta_source_tmst TIMESTAMPTZ NOT NULL,
    meta_insert_tmst TIMESTAMPTZ NOT NULL,
    meta_updated_tmst TIMESTAMPTZ NOT NULL
);
