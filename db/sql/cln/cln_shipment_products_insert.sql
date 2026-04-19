INSERT INTO cln.shipment_products (
    event_id,
    event_timestamp,
    event_name,
    payload_cln,
    raw_offset_id,
    meta_insert_timestamp,
    meta_update_timestamp,
    meta_source_file_path
)
VALUES (
    :event_id,
    :event_timestamp,
    :event_name,
    :payload_cln,
    :raw_offset_id,
    :meta_insert_timestamp,
    :meta_update_timestamp,
    :meta_source_file_path
)
ON CONFLICT (event_id) DO UPDATE SET
    payload_cln = excluded.payload_cln,
    meta_update_timestamp = excluded.meta_insert_timestamp,
    meta_source_file_path = excluded.meta_source_file_path,
    raw_offset_id = excluded.raw_offset_id,
    event_timestamp = excluded.event_timestamp,
    event_name = excluded.event_name