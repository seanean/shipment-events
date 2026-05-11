{{
    config(
        materialized='table',
        unique_key='meta_root_business_key',
        incremental_strategy='merge'
    )
}}

--pull in (relevant) records from CLN table.
WITH sp_cln as (
    SELECT 
        payload_cln->'event_data'->>'shipment_uuid' AS meta_root_business_key
        , event_tmst
        , event_name
        , event_type AS meta_source_event_type_lst
        , payload_cln
        , meta_source_file_path AS meta_source_latest_file_path
        , meta_source_file_path_lst
        , raw_offset_id
    FROM {{ source('shipment_events_cln', 'shipment_products') }}
    -- inc? only pull in new records
    {% if is_incremental() %}
        WHERE raw_offset_id >= (SELECT max(raw_offset_id) FROM {{ this }})
    {% endif %}
)

--inc? get records matching root keys in increment to append file paths
{% if is_incremental() %}
, sp_cln_and_matching_results AS (
    SELECT
        result.meta_root_business_key
        , result.event_tmst
        , result.event_name
        , result.meta_source_event_type_lst
        , result.payload_cln
        , result.meta_source_latest_file_path
        , result.meta_source_file_path_lst
        , result.raw_offset_id
    FROM {{ this }} AS result
    INNER JOIN sp_cln
        ON sp_cln.meta_root_business_key = result.meta_root_business_key
    UNION ALL
    SELECT * FROM sp_cln
)
{% endif %}

--get max raw_offset_id and append file paths based on...
    --full: cln
    --inc: cln increment + matching result records
, sp_grouped AS (
    SELECT
        meta_root_business_key
        , STRING_AGG(meta_source_file_path_lst, ',' ORDER BY meta_source_file_path_lst DESC) AS meta_source_file_path_lst
        , MAX(raw_offset_id) AS raw_offset_id
    FROM
    {% if is_incremental() %}
        sp_cln_and_matching_results
    {% else %}
        sp_cln
    {% endif %}
    GROUP BY
        meta_root_business_key
)

--take the newest rows + full path list to insert or merge 
, sp_latest AS (
    SELECT
        sp_cln.payload_cln
        , sp_cln.event_name
        , sp_cln.event_tmst
        , sp_cln.meta_source_latest_file_path
        , sp_cln.meta_source_event_type_lst
        , sp_grouped.meta_source_file_path_lst
        , sp_cln.meta_root_business_key
        , sp_cln.raw_offset_id
    FROM sp_cln
    INNER JOIN sp_grouped
        ON sp_cln.raw_offset_id = sp_grouped.raw_offset_id
)
SELECT *
FROM sp_latest