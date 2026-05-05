/*vars to pass in in this order because I'm using psycopg sql.SQL with .format
params_cur.from_timestamp_exclusive,
_CUR_BATCH_SIZE,
params_cur.run_id,
params_cur.batch_id,
params_cur.job_name,
params_cur.started_at,
params_cur.latest_timestamp,
params_cur.from_timestamp_exclusive,
params_cur.job_name,
params_cur.job_name
*/
BEGIN;

/* only for review:
SELECT * FROM cln.shipment_products;
*/

/* get data for given batch */
CREATE TEMP TABLE tmp_sp_batch ON COMMIT DROP AS
WITH sp_batch AS (
    SELECT
        event_id
        , payload_cln->'event_data'->>'shipment_uuid' AS meta_root_business_key
        , event_tmst
        , event_name
        , payload_cln
        , raw_offset_id
        , raw_offset_id_lst
        , meta_insert_tmst
        , meta_update_tmst
        , meta_source_file_path
        , meta_source_file_path_lst
    FROM cln.shipment_products
    WHERE event_tmst > {}--params_cur.from_timestamp_exclusive
    LIMIT {}--_CUR_BATCH_SIZE
)

/*
per business ID: //Note: for status, we will have to do this per event type, not just root key.
1) find newest offset
2) concatenate file paths
3) concatenate event IDs
*/
, sp_latest AS (
    SELECT
        meta_root_business_key
        , STRING_AGG(CONCAT(event_name, ': ', event_id), ',' ORDER BY CONCAT(event_name, ': ', event_id) DESC) AS meta_source_event_id_lst
        , STRING_AGG(meta_source_file_path_lst, ',' ORDER BY meta_source_file_path_lst DESC) AS meta_source_file_path_lst
        , MAX(event_tmst) AS event_tmst
    FROM sp_batch
    GROUP BY meta_root_business_key
)

/* populate temp_sp_batch: events to upsert to cur in original format */
, to_process AS (
    SELECT
        sp_batch.raw_offset_id
        , sp_batch.event_tmst
        , sp_batch.payload_cln
        , CONCAT(sp_batch.event_name, ': ', sp_batch.event_id) AS meta_source_latest_event_id
        , sp_batch.meta_source_file_path AS meta_source_latest_file_path
        , sp_latest.meta_source_event_id_lst
        , sp_latest.meta_source_file_path_lst
        , sp_batch.meta_root_business_key
    FROM sp_batch
    INNER JOIN sp_latest
        ON sp_batch.event_tmst = sp_latest.event_tmst
)
SELECT *
FROM to_process;


/*
only for review:
select * from tmp_sp_batch;
*/

/* tmp_sp_shipment: to upsert into cur.shipment*/
CREATE TEMP TABLE tmp_sp_shipment ON COMMIT DROP AS
SELECT
    sb.meta_root_business_key AS shipment_uuid
    , sb.payload_cln->'event_data'->>'shipment_business_id' AS shipment_business_id
    , sb.payload_cln->'event_data'->>'shipment_id' AS shipment_technical_id
    , sb.payload_cln->'event_data'->>'shipment_type' AS shipment_type
    , sb.meta_source_latest_event_id
    , sb.meta_source_latest_file_path
    , sb.meta_source_event_id_lst
    , sb.meta_source_file_path_lst
    , sb.meta_root_business_key
    , sb.event_tmst as meta_source_tmst
FROM tmp_sp_batch AS sb;

/*
only for review:
SELECT * FROM tmp_sp_shipment;
*/

/* cur.shipment upsert */
INSERT INTO cur.shipment (
    shipment_uuid
    , shipment_business_id
    , shipment_technical_id
    , shipment_type
    , meta_source_latest_event_id
    , meta_source_latest_file_path
    , meta_source_event_id_lst
    , meta_source_file_path_lst
    , meta_root_business_key
    , meta_source_tmst
    , meta_insert_tmst
    , meta_update_tmst
)
SELECT
    shipment_uuid
    , shipment_business_id
    , shipment_technical_id
    , shipment_type
    , meta_source_latest_event_id
    , meta_source_latest_file_path
    , meta_source_event_id_lst
    , meta_source_file_path_lst
    , meta_root_business_key
    , meta_source_tmst
    , NOW()
    , NOW()
FROM tmp_sp_shipment
ON CONFLICT (shipment_uuid) DO UPDATE SET
    shipment_business_id = excluded.shipment_business_id
    , shipment_technical_id = excluded.shipment_technical_id
    , shipment_type = excluded.shipment_type
    , meta_source_latest_event_id = excluded.meta_source_latest_event_id
    , meta_source_latest_file_path = excluded.meta_source_latest_file_path
    , meta_source_event_id_lst = (
        SELECT STRING_AGG(event_ids.meta_source_event_id, ',' ORDER BY meta_source_event_id DESC) AS meta_source_event_id_lst
        FROM (
            SELECT UNNEST(STRING_TO_ARRAY(cur.shipment.meta_source_event_id_lst, ',')) AS meta_source_event_id
            UNION
            SELECT UNNEST(STRING_TO_ARRAY(excluded.meta_source_event_id_lst, ',')) AS meta_source_event_id
        ) AS event_ids
    )
    , meta_source_file_path_lst = (
        SELECT STRING_AGG(event_ids.meta_source_file_path, ',' ORDER BY meta_source_file_path DESC) AS meta_source_file_path_lst
        FROM (
            SELECT UNNEST(STRING_TO_ARRAY(cur.shipment.meta_source_file_path_lst, ',')) AS meta_source_file_path
            UNION
            SELECT UNNEST(STRING_TO_ARRAY(excluded.meta_source_file_path_lst, ',')) AS meta_source_file_path
        ) AS event_ids
    )
    , meta_source_tmst = excluded.meta_source_tmst
    , meta_update_tmst = NOW();

/*
only for review:
SELECT * FROM CUR.shipment;
*/ 

/* tmp_sp_shipment_product: to upsert into cur.shipment_product */
CREATE TEMP TABLE tmp_sp_shipment_product ON COMMIT DROP AS
WITH sb_product AS (
    SELECT
        jsonb_array_elements(sb.payload_cln->'event_data'->'products') AS product
        , sb.meta_root_business_key AS shipment_uuid
        , sb.meta_source_latest_event_id
        , sb.meta_source_latest_file_path
        , sb.meta_source_event_id_lst
        , sb.meta_source_file_path_lst
        , sb.meta_root_business_key
        , sb.event_tmst as meta_source_tmst
    FROM tmp_sp_batch AS sb
)

SELECT
    product->>'shipment_product_uuid' AS shipment_product_uuid
    , shipment_uuid
    , product->>'product_id' AS product_id
    , (product->>'product_quantity')::integer AS product_qty
    , meta_source_latest_event_id
    , meta_source_latest_file_path
    , meta_source_event_id_lst
    , meta_source_file_path_lst
    , meta_root_business_key
    , meta_source_tmst
FROM sb_product;

/* cur.shipment_product upsert */
INSERT INTO cur.shipment_product (
    shipment_product_uuid
    , shipment_uuid
    , product_id
    , product_qty
    , meta_source_latest_event_id
    , meta_source_latest_file_path
    , meta_source_event_id_lst
    , meta_source_file_path_lst
    , meta_root_business_key
    , meta_source_tmst
    , meta_insert_tmst
    , meta_update_tmst
)
SELECT
    shipment_product_uuid
    , shipment_uuid
    , product_id
    , product_qty
    , meta_source_latest_event_id
    , meta_source_latest_file_path
    , meta_source_event_id_lst
    , meta_source_file_path_lst
    , meta_root_business_key
    , meta_source_tmst
    , NOW()
    , NOW()
FROM tmp_sp_shipment_product
ON CONFLICT (shipment_product_uuid) DO UPDATE SET
    product_id = excluded.product_id
    , product_qty = excluded.product_qty
    , meta_source_latest_event_id = excluded.meta_source_latest_event_id
    , meta_source_latest_file_path = excluded.meta_source_latest_file_path
    , meta_source_event_id_lst = (
        SELECT STRING_AGG(event_ids.meta_source_event_id, ',' ORDER BY meta_source_event_id DESC) AS meta_source_event_id_lst
        FROM (
            SELECT UNNEST(STRING_TO_ARRAY(cur.shipment_product.meta_source_event_id_lst, ',')) AS meta_source_event_id
            UNION
            SELECT UNNEST(STRING_TO_ARRAY(excluded.meta_source_event_id_lst, ',')) AS meta_source_event_id
        ) AS event_ids
    )
    , meta_source_file_path_lst = (
        SELECT STRING_AGG(event_ids.meta_source_file_path, ',' ORDER BY meta_source_file_path DESC) AS meta_source_file_path_lst
        FROM (
            SELECT UNNEST(STRING_TO_ARRAY(cur.shipment_product.meta_source_file_path_lst, ',')) AS meta_source_file_path
            UNION
            SELECT UNNEST(STRING_TO_ARRAY(excluded.meta_source_file_path_lst, ',')) AS meta_source_file_path
        ) AS event_ids
    )
    , meta_source_tmst = excluded.meta_source_tmst
    , meta_update_tmst = NOW();
/*
only for review:
SELECT * FROM cur.shipment_product;
*/

/* Orphan handling */
DELETE FROM cur.shipment_product
WHERE cur.shipment_product.meta_root_business_key IN (SELECT meta_root_business_key FROM tmp_sp_batch)
  AND (cur.shipment_product.shipment_product_uuid) NOT IN (
      SELECT shipment_product_uuid FROM tmp_sp_shipment_product
  );


/* add success into meta.pipeline_run */
INSERT INTO meta.pipeline_run (
        run_id
        , batch_id
        , job_name
        , status
        , started_at
        , finished_at
        , starting_from_timestamp_exclusive
        , from_timestamp_exclusive
        , to_timestamp_inclusive
        , rows_read
        , rows_written
        , error_message
        , traceback_message
        , meta_insert_tmst
    )
    VALUES (
        {}--params_cur.run_id
        , {}--params_cur.batch_id
        , {}--params_cur.job_name
        , 'success'
        , {}--params_cur.started_at
        , NOW()--finished_at
        , {}--params_cur.latest_timestamp
        , {}--params_cur.from_timestamp_exclusive
        , (SELECT MAX(event_tmst) FROM tmp_sp_batch)--to_timestamp_inclusive
        , (SELECT COUNT(1) FROM tmp_sp_batch)--rows_read
        , ((SELECT COUNT(1) FROM tmp_sp_shipment_product)+(SELECT COUNT(1) FROM tmp_sp_shipment))--rows_written
        , NULL--error_message
        , NULL--traceback_message
        , NOW()--meta_insert_tmst
    );
COMMIT;