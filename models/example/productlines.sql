{{ config(
    materialized='incremental',
    unique_key='productline'
) }}

WITH combined_data AS (
    SELECT
        s.productline,
        s.create_timestamp,
        s.update_timestamp,
        COALESCE(e.src_create_timestamp, s.create_timestamp) AS src_create_timestamp,
        COALESCE(s.update_timestamp, e.src_update_timestamp) AS src_update_timestamp,
        COALESCE(e.dw_product_line_id, 
                 ROW_NUMBER() OVER (ORDER BY s.productline) + COALESCE(MAX(e.dw_product_line_id) OVER (), 0)) AS dw_product_line_id,
        B.etl_batch_no,
        B.etl_batch_date,
        CASE
            WHEN s.productline IS NOT NULL THEN CURRENT_TIMESTAMP
            ELSE e.dw_create_timestamp
        END AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp
    FROM
        {{ source('devstage', 'ProductLines') }} AS s
    LEFT JOIN {{this}} AS e
        ON s.productline = e.productline
    CROSS JOIN {{ source('etl_metadata', 'batch_control') }} AS B
)

SELECT
    productline,
    dw_product_line_id,
    src_create_timestamp,
    src_update_timestamp,
    etl_batch_no,
    etl_batch_date,
    dw_update_timestamp,
    dw_create_timestamp
FROM combined_data

