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
        s.create_timestamp AS src_create_timestamp,
        s.update_timestamp AS src_update_timestamp,
        ROW_NUMBER() OVER () + COALESCE(MAX(e.dw_payment_id) OVER (), 0) AS dw_payment_id,
        C.dw_customer_id AS dw_customer_id,
        B.etl_batch_no,
        B.etl_batch_date,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CURRENT_TIMESTAMP AS dw_update_timestamp
    FROM
        {{ source('devstage', 'Payments') }} AS s
    JOIN 
        {{ this }} AS e
        ON s.checkNumber = e.checkNumber
    JOIN 
        {{ ref('customers') }} C
        ON s.customerNumber = C.src_customerNumber
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
