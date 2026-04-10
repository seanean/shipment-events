CREATE TABLE shipment_status (
    shipment_status_uuid STRING NOT NULL REFERENCES shipment (shipment_uuid),
    shipment_uuid STRING NOT NULL,
    shipment_status STRING NOT NULL,
    shipment_status_tmst TIMESTAMP NOT NULL,
    meta_source_event_ids STRING,
    meta_source_event_names STRING,
    meta_insert_timestamp TIMESTAMP,
    meta_updated_timestamp TIMESTAMP,
    PRIMARY KEY (shipment_status_uuid)
);
