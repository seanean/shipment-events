{{
    config(
        materialized='incremental',
        unique_key=['meta_root_business_key', 'event_name'],
        incremental_strategy='merge',
        schema='stg',
        indexes=[
            {'columns': ['raw_offset_id'], 'unique': True}
        ]
    )
}}

--pull in (relevant) records from CLN table.
WITH ss_cln as (
    SELECT 
        payload_cln->'event_data'->>'shipment_uuid' AS meta_root_business_key
        , event_tmst
        , event_name
        , event_type AS meta_source_event_type_lst
        , payload_cln
        , meta_source_file_path AS meta_source_latest_file_path
        , meta_source_file_path_lst
        , raw_offset_id
    FROM {{ source('shipment_events_cln', 'shipment_status') }}
    -- inc? only pull in new records
    {% if is_incremental() %}
        WHERE raw_offset_id >= (SELECT max(raw_offset_id) FROM {{ this }})
    {% endif %}
)

--inc? get records matching root keys in increment to append file paths
{% if is_incremental() %}
, ss_cln_and_matching_results AS (
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
    INNER JOIN ss_cln
        ON ss_cln.meta_root_business_key = result.meta_root_business_key
    UNION ALL
    SELECT * FROM ss_cln
)
{% endif %}

--get max raw_offset_id and append file paths based on...
    --full: cln
    --inc: cln increment + matching result records
, ss_grouped AS (
    SELECT
        meta_root_business_key
        , event_name
        , STRING_AGG(meta_source_file_path_lst, ',' ORDER BY meta_source_file_path_lst DESC) AS meta_source_file_path_lst
        , MAX(raw_offset_id) AS raw_offset_id
    FROM
    {% if is_incremental() %}
        ss_cln_and_matching_results
    {% else %}
        ss_cln
    {% endif %}
    GROUP BY
        meta_root_business_key
        , event_name
)

--take the newest rows + full path list to insert or merge 
, ss_latest AS (
    SELECT
        ss_cln.payload_cln
        , ss_cln.event_name
        , ss_cln.event_tmst
        , ss_cln.meta_source_latest_file_path
        , ss_cln.meta_source_event_type_lst
        , ss_grouped.meta_source_file_path_lst
        , ss_cln.meta_root_business_key
        , ss_cln.raw_offset_id
    FROM ss_cln
    INNER JOIN ss_grouped
        ON ss_cln.raw_offset_id = ss_grouped.raw_offset_id
            AND ss_cln.event_name = ss_grouped.event_name
)
SELECT *
FROM ss_latest