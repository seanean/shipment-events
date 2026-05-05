INSERT INTO raw.shipment_products (
    event_id,
    event_tmst,
    event_name,
    payload,
    meta_insert_tmst,
    meta_source_file_path
)
VALUES (
    :event_id,
    :event_tmst,
    :event_name,
    :payload,
    NOW(),
    :meta_source_file_path
)