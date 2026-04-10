CREATE TABLE [shipment] (
    [shipment_uuid] NVARCHAR(255) NOT NULL,
    [shipment_business_id] NVARCHAR(255) NOT NULL,
    [shipment_technical_id] NVARCHAR(255) NOT NULL,
    [shipment_type] NVARCHAR(255) NOT NULL,
    [meta_source_event_ids] NVARCHAR(255),
    [meta_source_event_names] NVARCHAR(255),
    [meta_insert_timestamp] DATETIME2,
    [meta_updated_timestamp] DATETIME2,
    PRIMARY KEY ([shipment_uuid])
);
