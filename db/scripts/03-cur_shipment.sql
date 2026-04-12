CREATE TABLE cur.shipment (
    shipment_uuid VARCHAR NOT NULL,
    shipment_business_id VARCHAR NOT NULL,
    shipment_technical_id VARCHAR NOT NULL,
    shipment_type VARCHAR NOT NULL,
    meta_source_event_ids VARCHAR,
    meta_source_event_names VARCHAR,
    meta_insert_timestamp TIMESTAMP,
    meta_updated_timestamp TIMESTAMP,
    PRIMARY KEY (shipment_uuid)
);
ALTER TABLE cur.shipment OWNER TO shrw;
