INSERT INTO cln.shipment_status (
    event_id,
    event_timestamp,
    event_name,
    payload,
    raw_offset_id,
    meta_insert_timestamp,
    meta_source_file_path
)
VALUES (
    :event_id,
    :event_timestamp,
    :event_name,
    :payload,
    :raw_offset_id,
    :meta_insert_timestamp,
    :meta_source_file_path
)