/*vars to pass in in this order because I'm using psycopg sql.SQL with .format
params_cur.from_id_exclusive,
_CUR_BATCH_SIZE,
params_cur.run_id,
params_cur.batch_id,
params_cur.job_name,
params_cur.started_at,
params_cur.latest_raw_offset_id,
params_cur.from_id_exclusive,
params_cur.job_name,
params_cur.job_name
*/
BEGIN;

/* only for review:
SELECT * FROM cln.shipment_status;
*/

/* get data for given batch */
CREATE TEMP TABLE tmp_ss_batch ON COMMIT DROP AS
WITH ss_batch AS (
    SELECT
        payload_cln->'event_data'->>'shipment_uuid' AS meta_root_business_key
        , raw_offset_id
        , event_tmst
        , event_name
        , event_type
        , payload_cln
        , meta_insert_tmst
        , meta_update_tmst
        , meta_source_file_path
        , meta_source_file_path_lst
    FROM cln.shipment_status
    WHERE raw_offset_id  > {}--params_cur.from_id_exclusive
    LIMIT {}--_CUR_BATCH_SIZE
)

/*
per business ID: //Note: for status, we will have to do this per event name (status type), not just root key.
1) find newest offset
2) concatenate file paths
3) concatenate event IDs
*/
, ss_latest AS (
    SELECT
        meta_root_business_key
        , event_name
        , STRING_AGG(meta_source_file_path_lst, ',' ORDER BY meta_source_file_path_lst DESC) AS meta_source_file_path_lst
        , MAX(raw_offset_id) AS raw_offset_id
    FROM ss_batch
    GROUP BY
        meta_root_business_key
        , event_name
)


/*
populate temp_ss_batch: events to upsert to cur in original format
ensure we take latest event per event_name
important:
    - in this project, each status type has its own event_name
    - if they did not (i.e. ShipmentCompleted, ShipmentComplete), we would need to do this on status instead.
*/

, to_process AS (
    SELECT
        ss_batch.payload_cln
        , ss_batch.event_name
        , ss_batch.event_tmst
        , ss_batch.meta_source_file_path AS meta_source_latest_file_path
        , ss_batch.event_type AS meta_source_event_type_lst
        , ss_latest.meta_source_file_path_lst
        , ss_batch.meta_root_business_key
        , ss_batch.raw_offset_id
    FROM ss_batch
    INNER JOIN ss_latest
        ON ss_batch.raw_offset_id = ss_latest.raw_offset_id
            AND ss_batch.event_name = ss_latest.event_name
)
SELECT *
FROM to_process;

/*
only for review:
select * from tmp_ss_batch;
*/

/* tmp_ss_shipment: to upsert into cur.shipment*/
CREATE TEMP TABLE tmp_ss_shipment ON COMMIT DROP AS
SELECT
    sb.meta_root_business_key AS shipment_uuid
    , sb.payload_cln->'event_data'->>'shipment_business_id' AS shipment_business_id
    , sb.payload_cln->'event_data'->>'shipment_id' AS shipment_technical_id
    , sb.payload_cln->'event_data'->>'shipment_type' AS shipment_type
    , sb.meta_source_latest_file_path
    , sb.meta_source_event_type_lst
    , sb.meta_source_file_path_lst
    , sb.meta_root_business_key
    , sb.event_tmst AS meta_source_tmst
FROM tmp_ss_batch AS sb;

/*
only for review:
SELECT * FROM tmp_ss_shipment;
*/

/* cur.shipment upsert */
INSERT INTO cur.shipment (
    shipment_uuid
    , shipment_business_id
    , shipment_technical_id
    , shipment_type
    , meta_source_latest_file_path
    , meta_source_event_type_lst
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
    , meta_source_latest_file_path
    , meta_source_event_type_lst
    , meta_source_file_path_lst
    , meta_root_business_key
    , meta_source_tmst
    , NOW()
    , NOW()
FROM tmp_ss_shipment
ON CONFLICT (shipment_uuid) DO UPDATE SET
    shipment_business_id = excluded.shipment_business_id
    , shipment_technical_id = excluded.shipment_technical_id
    , shipment_type = excluded.shipment_type
    , meta_source_latest_file_path = excluded.meta_source_latest_file_path
    , meta_source_event_type_lst = (
        SELECT STRING_AGG(event_types.meta_source_event_type, ',' ORDER BY meta_source_event_type DESC) AS meta_source_event_type_lst
        FROM (
            SELECT UNNEST(STRING_TO_ARRAY(cur.shipment.meta_source_event_type_lst, ',')) AS meta_source_event_type
            UNION
            SELECT UNNEST(STRING_TO_ARRAY(excluded.meta_source_event_type_lst, ',')) AS meta_source_event_type
        ) AS event_types
    )
    , meta_source_file_path_lst = (
        SELECT STRING_AGG(file_paths.meta_source_file_path, ',' ORDER BY meta_source_file_path DESC) AS meta_source_file_path_lst
        FROM (
            SELECT UNNEST(STRING_TO_ARRAY(cur.shipment.meta_source_file_path_lst, ',')) AS meta_source_file_path
            UNION
            SELECT UNNEST(STRING_TO_ARRAY(excluded.meta_source_file_path_lst, ',')) AS meta_source_file_path
        ) AS file_paths
    )
    , meta_source_tmst = excluded.meta_source_tmst
    , meta_update_tmst = NOW();

/*
only for review:
SELECT * FROM CUR.shipment;
*/ 

/*
tmp_ss_shipment_status: to upsert into cur.shipment_status
very light transformations. more just filling the role of separating logic vs upsert.
*/
CREATE TEMP TABLE tmp_ss_shipment_status ON COMMIT DROP AS
    SELECT
        sb.payload_cln->'event_data'->>'shipment_status_uuid' AS shipment_status_uuid
        , sb.meta_root_business_key AS shipment_uuid
        , sb.payload_cln->'event_data'->>'status' AS shipment_status
        , (sb.payload_cln->'event_data'->>'status_timestamp')::TIMESTAMPTZ AS shipment_status_tmst
        , sb.meta_source_latest_file_path
        , sb.meta_source_event_type_lst
        , sb.meta_source_file_path_lst
        , sb.meta_root_business_key
        , sb.event_tmst AS meta_source_tmst
    FROM tmp_ss_batch AS sb;

/* cur.shipment_status upsert */
INSERT INTO cur.shipment_status (
    shipment_status_uuid
    , shipment_uuid
    , shipment_status
    , shipment_status_tmst
    , meta_source_latest_file_path
    , meta_source_event_type_lst
    , meta_source_file_path_lst
    , meta_root_business_key
    , meta_source_tmst
    , meta_insert_tmst
    , meta_update_tmst
)
SELECT
    shipment_status_uuid
    , shipment_uuid
    , shipment_status
    , shipment_status_tmst
    , meta_source_latest_file_path
    , meta_source_event_type_lst
    , meta_source_file_path_lst
    , meta_root_business_key
    , meta_source_tmst
    , NOW()
    , NOW()
FROM tmp_ss_shipment_status
ON CONFLICT (shipment_status_uuid) DO UPDATE SET
    shipment_status = excluded.shipment_status
    , shipment_status_tmst = excluded.shipment_status_tmst
    , meta_source_latest_file_path = excluded.meta_source_latest_file_path
    , meta_source_event_type_lst = excluded.meta_source_event_type_lst--only shipment_status populates this table
    , meta_source_file_path_lst = (
        SELECT STRING_AGG(file_paths.meta_source_file_path, ',' ORDER BY meta_source_file_path DESC) AS meta_source_file_path_lst
        FROM (
            SELECT UNNEST(STRING_TO_ARRAY(cur.shipment_status.meta_source_file_path_lst, ',')) AS meta_source_file_path
            UNION
            SELECT UNNEST(STRING_TO_ARRAY(excluded.meta_source_file_path_lst, ',')) AS meta_source_file_path
        ) AS file_paths
    )
    , meta_source_tmst = excluded.meta_source_tmst
    , meta_update_tmst = NOW();
/*
only for review:
SELECT * FROM cur.shipment_status;
*/

/* add success into meta.pipeline_run */
INSERT INTO meta.pipeline_run (
        run_id
        , batch_id
        , job_name
        , status
        , started_at
        , finished_at
        , starting_from_id_exclusive
        , from_id_exclusive
        , to_id_inclusive
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
        , {}--params_cur.latest_raw_offset_id
        , {}--params_cur.from_id_exclusive
        , (SELECT MAX(raw_offset_id) FROM tmp_ss_batch)--to_id_inclusive
        , (SELECT COUNT(1) FROM tmp_ss_batch)--rows_read
        , ((SELECT COUNT(1) FROM tmp_ss_shipment_status)+(SELECT COUNT(1) FROM tmp_ss_shipment))--rows_written
        , NULL--error_message
        , NULL--traceback_message
        , NOW()--meta_insert_tmst
    );
COMMIT;