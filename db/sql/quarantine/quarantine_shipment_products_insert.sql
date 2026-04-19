INSERT INTO quarantine.shipment_products (
    event_id,
    event_timestamp,
    event_name,
    payload,
    error_message,
    traceback_message,
    meta_insert_timestamp,
    meta_source_file_path
)
VALUES (
    :event_id,
    :event_timestamp,
    :event_name,
    :payload,
    :error_message,
    :traceback_message,
    NOW(),
    :meta_source_file_path
)