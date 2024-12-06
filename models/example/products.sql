{{ config(
    materialized='incremental',  -- Use incremental model
    unique_key='src_productCode'  -- Key to check for new/updated rows
) }}

WITH new_data AS (
    SELECT 
        A.productCode,
        A.productName,
        A.productLine,
        A.productScale,
        A.productVendor,
        A.productDescription,
        A.quantityInStock,
        A.buyPrice,
        A.MSRP,
        PL.dw_product_line_id,
        A.create_timestamp,
        A.update_timestamp,
        1001 AS etl_batch_no,  -- Pass in ETL batch number via variable
        '2001-01-01' AS etl_batch_date -- Pass in ETL batch date via variable
    FROM devstage.Products A
    LEFT JOIN devdw.Products B ON A.productCode = B.src_productCode
    JOIN devdw.ProductLines PL ON A.productLine = PL.productLine
    WHERE B.src_productCode IS NULL
)

-- The main insert/select for incremental load
SELECT * FROM new_data

{% if is_incremental() %}
    -- This is for incremental mode; only insert new records
    WHERE src_productCode NOT IN (SELECT src_productCode FROM {{ this }})
{% endif %}
