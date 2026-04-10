CREATE TABLE [shipment_products] (
    [shipment_product_uuid] NVARCHAR(255) NOT NULL,
    [shipment_uuid] NVARCHAR(255) NOT NULL,
    [product_id] NVARCHAR(255) NOT NULL,
    [product_qty] INT NOT NULL,
    [meta_source_event_ids] NVARCHAR(255),
    [meta_source_event_names] NVARCHAR(255),
    [meta_insert_timestamp] DATETIME2,
    [meta_updated_timestamp] DATETIME2
);
