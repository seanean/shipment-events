{{
    config(
        materialized='incremental',
        unique_key='shipment_uuid',
        incremental_strategy='merge',
        merge_update_columns = ['shipment_uuid', 'shipment_business_id', 'shipment_technical_id', 'shipment_type', 'meta_source_latest_file_path', 'meta_source_event_type_lst', 'meta_source_file_path_lst', 'meta_root_business_key', 'meta_source_tmst', 'meta_update_tmst', 'meta_latest_sp_offset_id', 'meta_latest_ss_offset_id'],
        schema='cur',
        indexes=[{'columns': ['shipment_uuid'], 'unique': True}]
    )
}}

--bring in records from shipment_products
--inc? only the records newer than existing
WITH sp_input AS (
    SELECT
        stg_sp.meta_root_business_key AS shipment_uuid
        , stg_sp.payload_cln->'event_data'->>'shipment_business_id' AS shipment_business_id
        , stg_sp.payload_cln->'event_data'->>'shipment_id' AS shipment_technical_id
        , stg_sp.payload_cln->'event_data'->>'shipment_type' AS shipment_type
        , stg_sp.meta_source_latest_file_path
        , stg_sp.meta_source_event_type_lst
        , stg_sp.meta_source_file_path_lst
        , stg_sp.meta_root_business_key
        , stg_sp.event_tmst AS meta_source_tmst
        , stg_sp.raw_offset_id AS meta_latest_sp_offset_id
    FROM {{ ref('stg_sp') }} AS stg_sp
    {% if is_incremental() %}
        WHERE stg_sp.raw_offset_id >= (SELECT max(meta_latest_sp_offset_id) FROM {{ this }})
    {% endif %}
)

--bring in records from shipment_status
--inc? only the records newer than existing
, ss_input AS (
    SELECT
        stg_ss.meta_root_business_key AS shipment_uuid
        , stg_ss.payload_cln->'event_data'->>'shipment_business_id' AS shipment_business_id
        , stg_ss.payload_cln->'event_data'->>'shipment_id' AS shipment_technical_id
        , stg_ss.payload_cln->'event_data'->>'shipment_type' AS shipment_type
        , stg_ss.meta_source_latest_file_path
        , stg_ss.meta_source_event_type_lst
        , stg_ss.meta_source_file_path_lst
        , stg_ss.meta_root_business_key
        , stg_ss.event_tmst AS meta_source_tmst
        , stg_ss.raw_offset_id AS meta_latest_ss_offset_id
    FROM {{ ref('stg_ss') }} AS stg_ss
    {% if is_incremental() %}
        WHERE stg_ss.raw_offset_id >= (SELECT max(meta_latest_ss_offset_id) FROM {{ this }})
    {% endif %}
)

--combine sp / ss into one list (to be aggregated)
, shipment_input_without_result AS (
    SELECT
        sp_input.shipment_uuid
        , sp_input.shipment_business_id
        , sp_input.shipment_technical_id
        , sp_input.shipment_type
        , sp_input.meta_source_latest_file_path
        , sp_input.meta_source_event_type_lst
        , sp_input.meta_source_file_path_lst
        , sp_input.meta_root_business_key
        , sp_input.meta_source_tmst
        , sp_input.meta_latest_sp_offset_id
        , NULL AS meta_latest_ss_offset_id
    FROM sp_input
    UNION ALL
    SELECT
        ss_input.shipment_uuid
        , ss_input.shipment_business_id
        , ss_input.shipment_technical_id
        , ss_input.shipment_type
        , ss_input.meta_source_latest_file_path
        , ss_input.meta_source_event_type_lst
        , ss_input.meta_source_file_path_lst
        , ss_input.meta_root_business_key
        , ss_input.meta_source_tmst
        , NULL AS meta_latest_sp_offset_id
        , ss_input.meta_latest_ss_offset_id
    FROM ss_input
)

--inc? ALSO pull in existing records for same keys from increment
--needed for file path and event type aggregation
, shipment_input AS (
{% if is_incremental() %}
    SELECT
        result.shipment_uuid
        , result.shipment_business_id
        , result.shipment_technical_id
        , result.shipment_type
        , result.meta_source_latest_file_path
        , result.meta_source_event_type_lst
        , result.meta_source_file_path_lst
        , result.meta_root_business_key
        , result.meta_source_tmst
        , result.meta_latest_sp_offset_id
        , result.meta_latest_ss_offset_id
    FROM {{ this }} AS result
    INNER JOIN shipment_input_without_result
        ON shipment_input_without_result.shipment_uuid = result.shipment_uuid
    UNION ALL
{% endif %}
    SELECT * FROM shipment_input_without_result
)

--filter on shipments with >1 record
--won't need any list aggregation if there's only one record
, shipment_with_multiple AS (
    SELECT
        shipment_input.shipment_uuid
        , COUNT(1) AS num_records
    FROM shipment_input
    GROUP BY shipment_uuid
    HAVING COUNT(1) > 1
)

--explode / re-combine lists based on all records
, shipment_lst_explode AS (
    SELECT
        STRING_AGG(DISTINCT meta_source_event_type_lst,',' ORDER BY meta_source_event_type_lst ASC) AS meta_source_event_type_lst
        , STRING_AGG(DISTINCT meta_source_file_path_lst,',' ORDER BY meta_source_file_path_lst ASC) AS meta_source_file_path_lst
        , shipment_uuid
    FROM (
        SELECT DISTINCT
            UNNEST(string_to_array(shipment_input.meta_source_event_type_lst,',')) AS meta_source_event_type_lst
            , UNNEST(string_to_array(shipment_input.meta_source_file_path_lst,',')) AS meta_source_file_path_lst
            , shipment_input.shipment_uuid
        FROM shipment_input
            INNER JOIN shipment_with_multiple
                ON shipment_input.shipment_uuid = shipment_with_multiple.shipment_uuid
    )
    GROUP BY shipment_uuid
)

--latest timestamp -> decide which record to insert
--latest offsets -> facilitate incremental
--lsts -> if multiple records, take the list, otherwise, take the value from input.
, shipment_grouped AS (
    SELECT
        shipment_input.shipment_uuid
        , MAX(shipment_input.meta_source_tmst) AS latest_source_tmst
        , COALESCE(shipment_lst_explode.meta_source_event_type_lst, shipment_input.meta_source_event_type_lst) AS meta_source_event_type_lst
        , COALESCE(shipment_lst_explode.meta_source_file_path_lst, shipment_input.meta_source_file_path_lst) AS meta_source_file_path_lst
        , MAX(shipment_input.meta_latest_sp_offset_id) AS meta_latest_sp_offset_id
        , MAX(shipment_input.meta_latest_ss_offset_id) AS meta_latest_ss_offset_id
    FROM shipment_input
    LEFT JOIN shipment_lst_explode
        ON shipment_input.shipment_uuid = shipment_lst_explode.shipment_uuid
    GROUP BY
        shipment_input.shipment_uuid
        , COALESCE(shipment_lst_explode.meta_source_event_type_lst, shipment_input.meta_source_event_type_lst)
        , COALESCE(shipment_lst_explode.meta_source_file_path_lst, shipment_input.meta_source_file_path_lst)
)

--get final output, only taking version of shipment with latest tmst + aggregate fields across shipment records
, shipment_latest AS (
    SELECT
        shipment_input.shipment_uuid
        , shipment_input.shipment_business_id
        , shipment_input.shipment_technical_id
        , shipment_input.shipment_type
        , shipment_input.meta_source_latest_file_path
        , shipment_grouped.meta_source_event_type_lst
        , shipment_grouped.meta_source_file_path_lst
        , shipment_input.meta_root_business_key
        , shipment_input.meta_source_tmst
        , NOW() AS meta_insert_tmst
        , NOW() AS meta_update_tmst
        , shipment_grouped.meta_latest_sp_offset_id
        , shipment_grouped.meta_latest_ss_offset_id
        , ROW_NUMBER() OVER (
            PARTITION BY shipment_input.shipment_uuid
            ORDER BY shipment_input.meta_latest_ss_offset_id, shipment_grouped.meta_latest_sp_offset_id  DESC
        ) AS rn--take the latest record arbitrarily if there are multiple with the same tmst
    FROM shipment_input
    INNER JOIN shipment_grouped
        ON shipment_input.shipment_uuid = shipment_grouped.shipment_uuid
        AND shipment_input.meta_source_tmst = shipment_grouped.latest_source_tmst
)
SELECT * FROM shipment_latest
WHERE rn = 1