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
        1001 AS etl_batch_no,  -- Pass in ETL batch number via variable
        '2001-01-01' AS etl_batch_date,  -- Pass in ETL batch date via variable
        ROW_NUMBER() OVER (ORDER BY A.productCode) + COALESCE(MAX(existing_data.dw_product_id) OVER (), 0) AS dw_product_id
    FROM {{ source('devstage', 'Products') }} A
    JOIN {{ ref('productlines') }} PL ON A.productLine = PL.productLine
    LEFT JOIN {{ this }} existing_data ON A.productCode = existing_data.src_productCode
)

-- The main insert/select for incremental load
SELECT * 
FROM new_data

{% if is_incremental() %}
    WHERE src_productCode NOT IN (SELECT src_productCode FROM {{ this }})
{% endif %}
