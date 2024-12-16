{{ config(
    materialized='incremental'
) }}

WITH CTE AS
(
    -- Orders data (non-cancelled orders)
    SELECT CAST(o.orderDate AS DATE) AS summary_date,
        p.dw_product_id,
        1 AS customer_apd,
        SUM(od.quantityOrdered * od.priceEach) AS product_cost_amount,
        SUM(od.quantityOrdered * p.MSRP) AS product_mrp_amount,
        0 AS cancelled_product_qty,
        0 AS cancelled_cost_amount,
        0 AS cancelled_mrp_amount,
        0 AS cancelled_order_apd
    FROM {{ ref('products')}} p
    JOIN {{ ref('orderdetails')}} od ON p.dw_product_id = od.dw_product_id
    JOIN {{ ref('orders')}} o ON od.dw_order_id = o.dw_order_id
    cross join {{ source("etl_metadata", 'batch_control')}} B
    WHERE o.orderDate >= B.etl_batch_date
    GROUP BY CAST(o.orderDate AS DATE),
            p.dw_product_id

    UNION ALL

    -- Cancelled orders data
    SELECT CAST(o.cancelledDate AS DATE) AS summary_date,
        p.dw_product_id,
        1 AS customer_apd,
        0 AS product_cost_amount,
        0 AS product_mrp_amount,
        COUNT(DISTINCT o.dw_order_id) AS cancelled_product_qty,
        SUM(od.quantityOrdered * od.priceEach) AS cancelled_cost_amount,
        SUM(od.quantityOrdered * p.MSRP) AS cancelled_mrp_amount,
        1 AS cancelled_order_apd
    FROM {{ ref('products')}} p
    JOIN {{ ref('orderdetails')}} od ON p.dw_product_id = od.dw_product_id
    JOIN {{ ref('orders')}} o ON od.dw_order_id = o.dw_order_id
    cross join {{ source('etl_metadata', 'batch_control')}} B
    WHERE o.cancelledDate >= B.etl_batch_date
    GROUP BY CAST(o.cancelledDate AS DATE),
            p.dw_product_id
)
SELECT C.summary_date,
    C.dw_product_id,
    MAX(C.customer_apd) AS customer_apd,
    MAX(C.product_cost_amount) AS product_cost_amount,
    MAX(C.product_mrp_amount) AS product_mrp_amount,
    MAX(C.cancelled_product_qty) AS cancelled_product_qty,
    MAX(C.cancelled_cost_amount) AS cancelled_cost_amount,
    MAX(C.cancelled_mrp_amount) AS cancelled_mrp_amount,
    MAX(C.cancelled_order_apd) AS cancelled_order_apd,
    CURRENT_TIMESTAMP AS dw_create_timestamp,
    B.etl_batch_no AS etl_batch_no,  
    B.etl_batch_date AS etl_batch_date  
FROM CTE C
cross join {{source("etl_metadata", 'batch_control')}} B
GROUP BY C.summary_date,
        C.dw_product_id,
        B.etl_batch_no,
        B.etl_batch_date
