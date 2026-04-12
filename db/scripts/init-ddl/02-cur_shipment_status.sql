CREATE TABLE cur.shipment_status (
    shipment_status_uuid VARCHAR NOT NULL REFERENCES cur.shipment (shipment_uuid),
    shipment_uuid VARCHAR NOT NULL,
    shipment_status VARCHAR NOT NULL,
    shipment_status_tmst TIMESTAMP NOT NULL,
    meta_source_event_ids VARCHAR,
    meta_source_event_names VARCHAR,
    meta_insert_timestamp TIMESTAMP,
    meta_updated_timestamp TIMESTAMP,
    PRIMARY KEY (shipment_status_uuid)
);
ALTER TABLE cur.shipment_status OWNER TO shrw;
