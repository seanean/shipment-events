INSERT INTO cln.shipment_status (
    event_id,
    event_timestamp,
    event_name,
    payload_cln,
    raw_offset_id,
    raw_offset_id_list,
    meta_insert_timestamp,
    meta_update_timestamp,
    meta_source_file_path,
    meta_source_file_path_list
)
VALUES (
    :event_id,
    :event_timestamp,
    :event_name,
    :payload_cln,
    :raw_offset_id,
    :raw_offset_id,
    NOW(),
    :meta_update_timestamp,
    :meta_source_file_path,
    :meta_source_file_path
)
ON CONFLICT (event_id) DO UPDATE SET
    payload_cln = excluded.payload_cln,
    raw_offset_id = excluded.raw_offset_id,
    raw_offset_id_list = concat_ws(',', excluded.raw_offset_id, cln.shipment_status.raw_offset_id_list),
    event_timestamp = excluded.event_timestamp,
    event_name = excluded.event_name,
    meta_update_timestamp = NOW(),
    meta_source_file_path = excluded.meta_source_file_path,
    meta_source_file_path_list = concat_ws(',', excluded.meta_source_file_path, cln.shipment_status.meta_source_file_path)