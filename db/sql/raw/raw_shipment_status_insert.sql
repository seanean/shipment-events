INSERT INTO raw.shipment_status (
    event_id,
    event_timestamp,
    event_name,
    payload,
    meta_insert_timestamp,
    meta_source_file_path
)
VALUES (
    :event_id,
    :event_timestamp,
    :event_name,
    :payload,
    NOW(),
    :meta_source_file_path
)