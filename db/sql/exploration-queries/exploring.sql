select * from quarantine.shipment_status;
select * from quarantine.shipment_products;
select * from raw.shipment_status;
select * from raw.shipment_products;
select * from cln.shipment_status;
select * from cln.shipment_products;

select * from meta.pipeline_run;


select concat_ws(',',1);


SELECT table_schema, table_name
FROM information_schema.tables
where table_schema in ('cur', 'raw', 'quarantine', 'cln');
