INSERT INTO raw.shipment_status (
    event_id,
    event_tmst,
    event_name,
    event_type,
    payload,
    meta_insert_tmst,
    meta_source_file_path
)
VALUES (
    :event_id,
    :event_tmst,
    :event_name,
    :event_type,
    :payload,
    NOW(),
    :meta_source_file_path
)