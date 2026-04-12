INSERT INTO raw.shipment_products (
    event_id,
    event_timestamp,
    event_name,
    payload,
    meta_insert_timestamp
)
VALUES (
    :event_id,
    :event_timestamp,
    :event_name,
    :payload,
    :meta_insert_timestamp
)