INSERT INTO quarantine.shipment_products (
    event_id,
    event_timestamp,
    event_name,
    payload,
    error_message,
    traceback_message,
    meta_insert_timestamp
)
VALUES (
    :event_id,
    :event_timestamp,
    :event_name,
    :payload,
    :error_message,
    :traceback_message,
    :meta_insert_timestamp
)