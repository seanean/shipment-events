CREATE TABLE `shipment_status` (
    `shipment_status_uuid` TEXT NOT NULL,
    `shipment_uuid` TEXT NOT NULL REFERENCES `shipment` (`shipment_uuid`),
    `shipment_status` TEXT NOT NULL,
    `shipment_status_tmst` TIMESTAMP NOT NULL,
    `meta_source_latest_file_path` TEXT NOT NULL,
    `meta_source_event_type_lst` TEXT NOT NULL,
    `meta_source_file_path_lst` TEXT NOT NULL,
    `meta_root_business_key` TEXT NOT NULL,
    `meta_source_tmst` TIMESTAMP NOT NULL,
    `meta_insert_tmst` TIMESTAMP NOT NULL,
    `meta_updated_tmst` TIMESTAMP NOT NULL,
    PRIMARY KEY (`shipment_status_uuid`)
);
