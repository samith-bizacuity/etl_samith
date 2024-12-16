{{ config(
    materialized='incremental',
    unique_key='officeCode'
) }}

WITH combined_data AS (
    SELECT
        s.officeCode,
        s.city,
        s.phone,
        s.addressLine1,
        s.addressLine2,
        s.state,
        s.country,
        s.postalCode,
        s.territory,
        COALESCE(e.src_create_timestamp, s.create_timestamp) AS src_create_timestamp,
        COALESCE(s.update_timestamp, e.src_update_timestamp) AS src_update_timestamp,
        COALESCE(e.dw_office_id, 
                 ROW_NUMBER() OVER (ORDER BY s.officeCode) + COALESCE(MAX(e.dw_office_id) OVER (), 0)) AS dw_office_id,
        B.etl_batch_no,
        B.etl_batch_date,
        CASE
            WHEN s.officeCode IS NULL THEN CURRENT_TIMESTAMP
            ELSE e.dw_create_timestamp
        END AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp
    FROM
        {{ source('devstage', 'Offices') }} AS s
    LEFT JOIN {{this}} AS e
        ON s.officeCode = e.officeCode
    CROSS JOIN {{ source('etl_metadata', 'batch_control') }} AS B
)

SELECT
    officeCode,
    city,
    phone,
    addressLine1,
    addressLine2,
    state,
    country,
    postalCode,
    territory,
    dw_office_id,
    src_create_timestamp,
    src_update_timestamp,
    etl_batch_no,
    etl_batch_date,
    dw_update_timestamp,
    dw_create_timestamp
FROM combined_data
