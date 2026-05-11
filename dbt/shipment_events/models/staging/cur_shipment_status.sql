{{
    config(
        materialized='table',
        unique_key='shipment_status_uuid',
        incremental_strategy='merge',
        schema='cur'
    )
}}

SELECT
    stg_ss.payload_cln->'event_data'->>'shipment_status_uuid' AS shipment_status_uuid
        , stg_ss.meta_root_business_key AS shipment_uuid
        , stg_ss.payload_cln->'event_data'->>'status' AS shipment_status
        , (stg_ss.payload_cln->'event_data'->>'status_timestamp')::TIMESTAMPTZ AS shipment_status_tmst
        , stg_ss.meta_source_latest_file_path
        , stg_ss.meta_source_event_type_lst
        , stg_ss.meta_source_file_path_lst
        , stg_ss.meta_root_business_key
        , stg_ss.raw_offset_id AS meta_raw_offset_id
        , stg_ss.event_tmst AS meta_source_tmst
        , NOW() AS meta_insert_tmst
        , NOW() AS meta_update_tmst
FROM {{ ref('stg_ss') }} AS stg_ss
{% if is_incremental() %}
    WHERE stg_ss.meta_raw_offset_id > (SELECT MAX(meta_raw_offset_id) FROM {{ this }})
{% endif %}