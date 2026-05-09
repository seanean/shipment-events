CREATE TABLE [shipment_product] (
    [shipment_product_uuid] NVARCHAR(MAX) NOT NULL,
    [shipment_uuid] NVARCHAR(MAX) NOT NULL REFERENCES [shipment] ([shipment_uuid]),
    [product_id] NVARCHAR(MAX) NOT NULL,
    [product_qty] INT NOT NULL,
    [meta_source_latest_file_path] NVARCHAR(MAX) NOT NULL,
    [meta_source_event_type_lst] NVARCHAR(MAX) NOT NULL,
    [meta_source_file_path_lst] NVARCHAR(MAX) NOT NULL,
    [meta_root_business_key] NVARCHAR(MAX) NOT NULL,
    [meta_source_tmst] DATETIMEOFFSET NOT NULL,
    [meta_insert_tmst] DATETIMEOFFSET NOT NULL,
    [meta_updated_tmst] DATETIMEOFFSET NOT NULL,
    PRIMARY KEY ([shipment_product_uuid])
);
