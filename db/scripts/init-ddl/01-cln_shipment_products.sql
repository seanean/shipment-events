CREATE TABLE cln.shipment_products (
    event_id VARCHAR NOT NULL PRIMARY KEY,
    event_tmst TIMESTAMPTZ NOT NULL,
    event_name VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    payload_cln JSONB NOT NULL,
    raw_offset_id BIGINT NOT NULL,
    raw_offset_id_lst VARCHAR NOT NULL,
    meta_insert_tmst TIMESTAMPTZ NOT NULL,
    meta_update_tmst TIMESTAMPTZ,
    meta_source_file_path VARCHAR NOT NULL,
    meta_source_file_path_lst VARCHAR NOT NULL
);
ALTER TABLE cln.shipment_products OWNER TO shrw;
CREATE INDEX idx_cln_shipment_products_event_id ON cln.shipment_products (event_id);
