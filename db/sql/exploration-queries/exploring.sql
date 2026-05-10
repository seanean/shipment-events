select * from quarantine.shipment_status;
select * from quarantine.shipment_products;
select * from raw.shipment_status;

select * from raw.shipment_products;
select * from cln.shipment_status;

select * from cln.shipment_products;
select * from cur.shipment;
select * from cur.stg_ss;


select * from meta.pipeline_run;

--string cat, for cln
select concat_ws(',',1);

--all tables
SELECT table_schema, table_name
FROM information_schema.tables
where table_schema in ('cur', 'raw', 'quarantine', 'cln');

--checking json syntax
SELECT
    sp.event_id
    , jsonb_array_elements(sp.payload_cln->'event_data'->'products') as p
FROM cln.shipment_products as sp
where sp.event_id = 'e2'
;


--checking logic to split an array, combine, provide distinct values, rebuild
select string_agg(x,',' order by x desc)
from(
select unnest(string_to_array('1,2,3',',')) as x
UNION
select unnest(string_to_array('2,3,4,5',',')) as x)
;


--checking select within values()
select 'yolo' as yolo, 1 as aaa
into temp table bob;

select * from bob;

insert into bob
values
('fart', (select count(1) from bob));



--testing my 'is there still stuff to query? query--
SELECT
    (
        SELECT COUNT(1)
        FROM cln.shipment_products
        WHERE raw_offset_id > (
            SELECT MAX(to_timestamp_inclusive)
            FROM meta.pipeline_run 
            WHERE job_name = 'curate_shipment_products_dev'--params_cur.job_name
                AND status = 'success'
            )
    );
+ 
    (
        SELECT COUNT(1)
        FROM cln.shipment_status
        WHERE raw_offset_id > (
            SELECT MAX(to_timestamp_inclusive)
            FROM meta.pipeline_run 
            WHERE job_name = 'curate_shipment_status_dev'--params_cur.job_name
                AND status = 'success'
            )
    );

select * from meta.pipeline_run;
select * from cur.shipment_product;
select * from cln.shipment_products;

--testing dropping a tmp on commit
begin;
CREATE TEMP TABLE tmp_yolo ON COMMIT DROP AS
WITH a as (select 1 as x)
select a.x from a
;
select * from tmp_yolo;
commit;