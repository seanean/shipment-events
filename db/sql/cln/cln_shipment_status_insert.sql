INSERT INTO cln.shipment_status (
    event_id,
    event_tmst,
    event_name,
    event_type,
    payload_cln,
    raw_offset_id,
    raw_offset_id_lst,
    meta_insert_tmst,
    meta_update_tmst,
    meta_source_file_path,
    meta_source_file_path_lst
)
VALUES (
    :event_id,
    :event_tmst,
    :event_name,
    :event_type,
    :payload_cln,
    :raw_offset_id,
    :raw_offset_id,
    NOW(),
    NOW(),
    :meta_source_file_path,
    :meta_source_file_path
)
ON CONFLICT (event_id) DO UPDATE SET
    payload_cln = excluded.payload_cln,
    raw_offset_id = excluded.raw_offset_id,
    raw_offset_id_lst = concat_ws(',', excluded.raw_offset_id, cln.shipment_status.raw_offset_id_lst),
    event_tmst = excluded.event_tmst,
    event_name = excluded.event_name,
    event_type = excluded.event_type,
    meta_update_tmst = NOW(),
    meta_source_file_path = excluded.meta_source_file_path,
    meta_source_file_path_lst = (
        SELECT STRING_AGG(file_paths.meta_source_file_path, ',' ORDER BY meta_source_file_path DESC) AS meta_source_file_path_lst
        FROM (
            SELECT UNNEST(STRING_TO_ARRAY(cln.shipment_status.meta_source_file_path_lst, ',')) AS meta_source_file_path
            UNION
            SELECT UNNEST(STRING_TO_ARRAY(excluded.meta_source_file_path_lst, ',')) AS meta_source_file_path
        ) AS file_paths
    )