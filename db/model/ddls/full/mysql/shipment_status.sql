CREATE TABLE `shipment_status` (
    `shipment_status_uuid` VARCHAR(255) NOT NULL REFERENCES `shipment` (`shipment_uuid`),
    `shipment_uuid` VARCHAR(255) NOT NULL,
    `shipment_status` VARCHAR(255) NOT NULL,
    `shipment_status_tmst` DATETIME NOT NULL,
    `meta_source_event_ids` VARCHAR(255),
    `meta_source_event_names` VARCHAR(255),
    `meta_insert_timestamp` DATETIME,
    `meta_updated_timestamp` DATETIME,
    PRIMARY KEY (`shipment_status_uuid`)
);
