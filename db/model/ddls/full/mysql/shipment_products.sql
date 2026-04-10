CREATE TABLE `shipment_products` (
    `shipment_product_uuid` VARCHAR(255) NOT NULL REFERENCES `shipment` (`shipment_uuid`),
    `shipment_uuid` VARCHAR(255) NOT NULL,
    `product_id` VARCHAR(255) NOT NULL,
    `product_qty` INT NOT NULL,
    `meta_source_event_ids` VARCHAR(255),
    `meta_source_event_names` VARCHAR(255),
    `meta_insert_timestamp` DATETIME,
    `meta_updated_timestamp` DATETIME,
    PRIMARY KEY (`shipment_product_uuid`)
);
