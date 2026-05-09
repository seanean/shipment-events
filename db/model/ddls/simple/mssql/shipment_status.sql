CREATE TABLE [shipment_status] (
    [shipment_status_uuid] NVARCHAR(MAX) NOT NULL,
    [shipment_uuid] NVARCHAR(MAX) NOT NULL,
    [shipment_status] NVARCHAR(MAX) NOT NULL,
    [shipment_status_tmst] DATETIMEOFFSET NOT NULL,
    [meta_source_latest_file_path] NVARCHAR(MAX) NOT NULL,
    [meta_source_event_type_lst] NVARCHAR(MAX) NOT NULL,
    [meta_source_file_path_lst] NVARCHAR(MAX) NOT NULL,
    [meta_root_business_key] NVARCHAR(MAX) NOT NULL,
    [meta_source_tmst] DATETIMEOFFSET NOT NULL,
    [meta_insert_tmst] DATETIMEOFFSET NOT NULL,
    [meta_updated_tmst] DATETIMEOFFSET NOT NULL
);
