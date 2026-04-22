CREATE TABLE [shipment_product] (
    [shipment_product_uuid] NVARCHAR(MAX) NOT NULL,
    [shipment_uuid] NVARCHAR(MAX) NOT NULL,
    [product_id] NVARCHAR(MAX) NOT NULL,
    [product_qty] INT NOT NULL,
    [meta_source_event_names] NVARCHAR(MAX) NOT NULL,
    [meta_source_latest_event_id] NVARCHAR(MAX) NOT NULL,
    [meta_source_event_id_lst] NVARCHAR(MAX) NOT NULL,
    [meta_source_file_path_lst] NVARCHAR(MAX) NOT NULL,
    [meta_root_business_key] NVARCHAR(MAX) NOT NULL,
    [meta_insert_timestamp] DATETIMEOFFSET NOT NULL,
    [meta_update_timestamp] DATETIMEOFFSET
);
