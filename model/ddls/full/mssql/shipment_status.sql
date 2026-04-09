CREATE TABLE [shipment_status] (
    [shipment_status_uuid] NVARCHAR(255) NOT NULL REFERENCES [shipment] ([shipment_uuid]),
    [shipment_uuid] NVARCHAR(255) NOT NULL,
    [shipment_status] NVARCHAR(255) NOT NULL,
    [shipment_status_tmst] DATETIME2 NOT NULL,
    [meta_source_event_ids] NVARCHAR(255),
    [meta_source_event_names] NVARCHAR(255),
    [meta_insert_timestamp] DATETIME2,
    [meta_updated_timestamp] DATETIME2,
    PRIMARY KEY ([shipment_status_uuid])
);
