select * from quarantine.shipment_status;
select * from quarantine.shipment_products;
select * from raw.shipment_status;

select * from raw.shipment_products;
select * from cln.shipment_status;
select * from cln.shipment_products;

select * from cur.shipment;
select * from cur.shipment_status;
select * from cur.shipment_product;
135656

--dbt tables
select * from dbt_stg.stg_ss;
select * from dbt_stg.stg_sp;
select * from dbt_cur.cur_shipment_products;
select * from dbt_cur.cur_shipment_status;
select * from dbt_cur.cur_shipment;


SELECT has_database_privilege('shrw', current_database(), 'CONNECT');

select schema_name
from information_schema.schemata
;

--meta
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



--testing logic for string agg for dbt
create temp table test_dbt (
  event_type_lst text,
  path_lst text,
  shipment_uuid int,
  meta_source_tmst timestamp
);

insert into test_dbt (event_type_lst, path_lst, shipment_uuid, meta_source_tmst) values
  ('event1,event2', 'path1,path2', 1, '2025-12-05'),
  ('event1', 'path1', 1, '2025-12-01'),
  ('event2', 'path2', 1, '2025-12-01'),
  ('event2', 'path2', 2, '2025-12-01'),
  ('event3', 'path3', 2, '2025-12-05');

select * from test_dbt;



-- select string_agg(x,',' order by x desc)
-- from(
select distinct unnest(string_to_array(test_dbt.event_type_lst,',')) as event_type, shipment_uuid
from test_dbt;
-- );



select string_agg(event_types,',' order by event_types asc)
from (
select distinct unnest(string_to_array(test_dbt.event_type_lst,',')) as event_types
from test_dbt
);



select string_agg(event_types,',' order by event_types asc), string_agg(path_lst,',' order by path_lst asc), shipment_uuid
from (
    select distinct
        unnest(string_to_array(test_dbt.event_type_lst,',')) as event_types
        , unnest(string_to_array(test_dbt.path_lst,',')) as path_lst
        , test_dbt.shipment_uuid
    from test_dbt
)
group by shipment_uuid;


create temp table test_dbt2 (
  meta_source_event_type_lst text,
  meta_source_file_path_lst text,
  shipment_uuid int
);

insert into test_dbt2 (meta_source_event_type_lst, meta_source_file_path_lst, shipment_uuid) values
  ('status,product', 'path1,path2', 1),
  ('status', 'path1', 1),
  ('product', 'path2', 1),
  ('status', 'path2', 2),
  ('product', 'path3', 2),
  ('status,product', 'path4', 3),
  ('product,status', 'path5', 3);

select * from test_dbt2;

    SELECT
        STRING_AGG(meta_source_event_type_lst,',' ORDER BY meta_source_event_type_lst ASC) AS meta_source_event_type_lst
        , STRING_AGG(meta_source_file_path_lst,',' ORDER BY meta_source_file_path_lst ASC) AS meta_source_file_path_lst
        , shipment_uuid
    FROM (
        SELECT DISTINCT
            UNNEST(string_to_array(shipment_input.meta_source_event_type_lst,',')) AS meta_source_event_type_lst
            , UNNEST(string_to_array(shipment_input.meta_source_file_path_lst,',')) AS meta_source_file_path_lst
            , shipment_input.shipment_uuid
        FROM test_dbt2 AS shipment_input
    )
    GROUP BY shipment_uuid;

    
        SELECT DISTINCT
            UNNEST(string_to_array(shipment_input.meta_source_event_type_lst,',')) AS meta_source_event_type_lst
            , UNNEST(string_to_array(shipment_input.meta_source_file_path_lst,',')) AS meta_source_file_path_lst
            , shipment_input.shipment_uuid
        FROM test_dbt2 AS shipment_input;
