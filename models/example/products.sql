{{ config(
    materialized='incremental',
    unique_key='src_productCode'
) }}

WITH new_data AS (
    SELECT 
        A.productCode AS src_productCode,
        A.productName,
        A.productLine,
        A.productScale,
        A.productVendor,
        A.quantityInStock,
        A.buyPrice,
        A.MSRP,
        PL.dw_product_line_id,
        A.create_timestamp as src_create_timestamp,
        A.update_timestamp as src_update_timestamp,
        CURRENT_TIMESTAMP as dw_update_timestamp,
        CASE
            WHEN e.src_productCode IS NULL THEN CURRENT_TIMESTAMP
            ELSE e.dw_create_timestamp
        END AS dw_create_timestamp,
        B.etl_batch_no,  -- Pass in ETL batch number via variable
        B.etl_batch_date,  -- Pass in ETL batch date via variable
        coalesce(e.dw_product_id, ROW_NUMBER() OVER (ORDER BY A.productCode) + COALESCE(MAX(e.dw_product_id) OVER (), 0)) AS dw_product_id
    FROM {{ source('devstage', 'Products') }} A
    JOIN {{ ref('productlines') }} PL ON A.productLine = PL.productLine
    LEFT JOIN {{ this }} e ON A.productCode = e.src_productCode
    CROSS JOIN {{ source('etl_metadata', 'batch_control') }} B
)

-- The main insert/select for incremental load
SELECT * 
FROM new_data