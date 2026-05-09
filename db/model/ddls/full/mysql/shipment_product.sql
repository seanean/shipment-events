CREATE TABLE `shipment_product` (
    `shipment_product_uuid` TEXT NOT NULL,
    `shipment_uuid` TEXT NOT NULL REFERENCES `shipment` (`shipment_uuid`),
    `product_id` TEXT NOT NULL,
    `product_qty` INT NOT NULL,
    `meta_source_latest_file_path` TEXT NOT NULL,
    `meta_source_event_type_lst` TEXT NOT NULL,
    `meta_source_file_path_lst` TEXT NOT NULL,
    `meta_root_business_key` TEXT NOT NULL,
    `meta_source_tmst` TIMESTAMP NOT NULL,
    `meta_insert_tmst` TIMESTAMP NOT NULL,
    `meta_updated_tmst` TIMESTAMP NOT NULL,
    PRIMARY KEY (`shipment_product_uuid`)
);
