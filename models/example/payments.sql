{{ config(
    materialized='incremental',
    unique_key='src_customerNumber, checkNumber'
) }}

WITH combined_data AS (
    SELECT
        s.customerNumber as src_customerNumber,
        s.checkNumber,
        s.paymentDate,
        s.amount,
        COALESCE(e.src_create_timestamp, s.create_timestamp) AS src_create_timestamp,
        COALESCE(s.update_timestamp, e.src_update_timestamp) AS src_update_timestamp,
        COALESCE(e.dw_payment_id, 
                 ROW_NUMBER() OVER (ORDER BY s.checkNumber) + COALESCE(MAX(e.dw_payment_id) OVER (), 0)) AS dw_payment_id,
        COALESCE(e.dw_customer_id, s.customerNumber) AS dw_customer_id,
        B.etl_batch_no,
        B.etl_batch_date,
        CASE
            WHEN s.checkNumber IS NOT NULL THEN CURRENT_TIMESTAMP
            ELSE e.dw_create_timestamp
        END AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp
    FROM
        {{ source('devstage', 'Payments') }} AS s
    LEFT JOIN {{this}} AS e
        ON s.checkNumber = e.checkNumber
    CROSS JOIN {{ source('etl_metadata', 'batch_control') }} AS B
)

SELECT
    src_customerNumber,
    checkNumber,
    paymentDate,
    amount,
    dw_payment_id,
    dw_customer_id,
    src_create_timestamp,
    src_update_timestamp,
    etl_batch_no,
    etl_batch_date,
    dw_update_timestamp,
    dw_create_timestamp
FROM combined_data
