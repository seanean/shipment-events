CREATE TABLE [shipment] (
    [shipment_uuid] NVARCHAR(MAX) NOT NULL,
    [shipment_business_id] NVARCHAR(MAX) NOT NULL,
    [shipment_technical_id] NVARCHAR(MAX) NOT NULL,
    [shipment_type] NVARCHAR(MAX) NOT NULL,
    [meta_source_latest_file_path] NVARCHAR(MAX) NOT NULL,
    [meta_source_event_type_lst] NVARCHAR(MAX) NOT NULL,
    [meta_source_file_path_lst] NVARCHAR(MAX) NOT NULL,
    [meta_root_business_key] NVARCHAR(MAX) NOT NULL,
    [meta_source_tmst] DATETIMEOFFSET NOT NULL,
    [meta_insert_tmst] DATETIMEOFFSET NOT NULL,
    [meta_update_tmst] DATETIMEOFFSET NOT NULL
);
