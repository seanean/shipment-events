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


SELECT
    sp.event_id
    , jsonb_array_elements(sp.payload_cln->'event_data'->'products') as p
FROM cln.shipment_products as sp
where sp.event_id = 'e2'
;

select string_agg(x,',' order by x desc)
from(
select unnest(string_to_array('1,2,3',',')) as x
UNION
select unnest(string_to_array('2,3,4,5',',')) as x)
;

select count(1) from cln.shipment_status;

SELECT COUNT(1) FROM tmp_sp_batch;

select 'yolo' as yolo, 1 as aaa
into temp table bob;

select * from bob;

insert into bob
values
('fart', (select count(1) from bob))