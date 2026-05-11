{{
    config(
        materialized='table',
        unique_key='shipment_product_uuid',
        incremental_strategy='merge',
        schema='cur',
        post_hook="
            DELETE FROM {{ this }} AS existing
            WHERE existing.meta_root_business_key IN (
                SELECT current.meta_root_business_key
                FROM {{ ref('stg_sp') }} AS current
            )
            AND existing.shipment_product_uuid NOT IN (
                SELECT product->>'shipment_product_uuid' AS shipment_product_uuid
                FROM (
                    SELECT
                        jsonb_array_elements(current.payload_cln->'event_data'->'products') AS product
                        FROM {{ ref('stg_sp') }} AS current
                    ) AS products
            )"
    )
}}

WITH stg_sp_to_array AS (
    SELECT
        jsonb_array_elements(stg_sp.payload_cln->'event_data'->'products') AS product
        , stg_sp.meta_root_business_key AS shipment_uuid
        , stg_sp.meta_source_latest_file_path
        , stg_sp.meta_source_event_type_lst
        , stg_sp.meta_source_file_path_lst
        , stg_sp.meta_root_business_key
        , stg_sp.event_tmst as meta_source_tmst
        , stg_sp.raw_offset_id AS meta_raw_offset_id
    FROM {{ ref('stg_sp') }} AS stg_sp
    {% if is_incremental() %}
    WHERE stg_sp.meta_raw_offset_id > (SELECT MAX(meta_raw_offset_id) FROM {{ this }})
    {% endif %}
)

, stg_sp_p AS (
SELECT
    product->>'shipment_product_uuid' AS shipment_product_uuid
    , shipment_uuid
    , product->>'product_id' AS product_id
    , (product->>'product_quantity')::integer AS product_qty
    , meta_source_latest_file_path
    , meta_source_event_type_lst
    , meta_source_file_path_lst
    , meta_root_business_key
    , meta_source_tmst
    , NOW() AS meta_insert_tmst
    , NOW() AS meta_update_tmst
    , meta_raw_offset_id
FROM stg_sp_to_array
)

SELECT * FROM stg_sp_p