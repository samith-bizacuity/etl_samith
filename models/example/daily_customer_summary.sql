{{ config(
    materialized='incremental',
    unique_key=['summary_date', 'dw_customer_id']
) }}

WITH CTE AS
(
    -- Orders data
    SELECT CAST(o.orderDate AS DATE) AS summary_date,
        o.dw_customer_id,
        COUNT(DISTINCT o.dw_order_id) AS order_count,
        1 AS order_apd,
        SUM(od.priceEach * od.quantityOrdered) AS order_amount,
        SUM(p.buyPrice * od.quantityOrdered) AS order_cost_amount,
        SUM(p.MSRP * od.quantityOrdered) AS order_mrp_amount,
        COUNT(DISTINCT od.src_productCode) AS products_ordered_qty,
        SUM(od.quantityOrdered) AS products_items_qty,
        0 AS cancelled_order_count,
        0 AS cancelled_order_amount,
        0 AS cancelled_order_apd,
        0 AS shipped_order_count,
        0 AS shipped_order_amount,
        0 AS shipped_order_apd,
        0 AS payment_apd,
        0 AS payment_amount,
        0 AS new_customer_apd,
        0 AS new_customer_paid_apd
    FROM {{ ref('orders')}} o
    JOIN {{ ref('orderdetails')}} od ON o.dw_order_id = od.dw_order_id
    JOIN {{ ref('products')}} p ON od.dw_product_id = p.dw_product_id
    cross join {{ source('etl_metadata', 'batch_control')}} B
    WHERE o.orderDate >= B.etl_batch_date  
    GROUP BY CAST(o.orderDate AS DATE),
            o.dw_customer_id

    UNION ALL

    -- Cancelled orders data
    SELECT CAST(o.cancelledDate AS DATE) AS summary_date,
        o.dw_customer_id,
        0 AS order_count,
        0 AS order_apd,
        0 AS order_amount,
        0 AS order_cost_amount,
        0 AS order_mrp_amount,
        0 AS products_ordered_qty,
        0 AS products_items_qty,
        COUNT(DISTINCT o.dw_order_id) AS cancelled_order_count,
        SUM(od.priceEach * od.quantityOrdered) AS cancelled_order_amount,
        1 AS cancelled_order_apd,
        0 AS shipped_order_count,
        0 AS shipped_order_amount,
        0 AS shipped_order_apd,
        0 AS payment_apd,
        0 AS payment_amount,
        0 AS new_customer_apd,
        0 AS new_customer_paid_apd
    FROM {{ ref('orders')}} o
    JOIN {{ ref('orderdetails')}} od ON o.dw_order_id = od.dw_order_id
    cross join {{ source('etl_metadata', 'batch_control')}} B
    WHERE o.cancelledDate >= B.etl_batch_date
    GROUP BY CAST(o.cancelledDate AS DATE),
            o.dw_customer_id

    UNION ALL

    -- Shipped orders data
    SELECT CAST(o.shippedDate AS DATE) AS summary_date,
        o.dw_customer_id,
        0 AS order_count,
        0 AS order_apd,
        0 AS order_amount,
        0 AS order_cost_amount,
        0 AS order_mrp_amount,
        0 AS products_ordered_qty,
        0 AS products_items_qty,
        0 AS cancelled_order_count,
        0 AS cancelled_order_amount,
        0 AS cancelled_order_apd,
        COUNT(DISTINCT o.dw_order_id) AS shipped_order_count,
        SUM(od.priceEach * od.quantityOrdered) AS shipped_order_amount,
        1 AS shipped_order_apd,
        0 AS payment_apd,
        0 AS payment_amount,
        0 AS new_customer_apd,
        0 AS new_customer_paid_apd
    FROM {{ ref('orders')}} o
    JOIN {{ ref('orderdetails')}} od ON o.dw_order_id = od.dw_order_id
    cross join {{ source('etl_metadata', 'batch_control')}} B
    WHERE o.shippedDate >= B.etl_batch_date
    AND o.status = 'Shipped'
    GROUP BY CAST(o.shippedDate AS DATE),
            o.dw_customer_id

    UNION ALL

    -- Payments data
    SELECT CAST(p.paymentDate AS DATE) AS summary_date,
        p.dw_customer_id,
        0 AS order_count,
        0 AS order_apd,
        0 AS order_amount,
        0 AS order_cost_amount,
        0 AS order_mrp_amount,
        0 AS products_ordered_qty,
        0 AS products_items_qty,
        0 AS cancelled_order_count,
        0 AS cancelled_order_amount,
        0 AS cancelled_order_apd,
        0 AS shipped_order_count,
        0 AS shipped_order_amount,
        0 AS shipped_order_apd,
        1 AS payment_apd,
        SUM(p.amount) AS payment_amount,
        0 AS new_customer_apd,
        0 AS new_customer_paid_apd
    FROM devdw.Payments p
    cross join {{ source('etl_metadata', 'batch_control')}} B
    WHERE p.paymentDate >= B.etl_batch_date
    GROUP BY CAST(p.paymentDate AS DATE),
            p.dw_customer_id

    UNION ALL

    -- New customer data
    SELECT CAST(c.src_create_timestamp AS DATE) AS summary_date,
        c.dw_customer_id,
        0 AS order_count,
        0 AS order_apd,
        0 AS order_amount,
        0 AS order_cost_amount,
        0 AS order_mrp_amount,
        0 AS products_ordered_qty,
        0 AS products_items_qty,
        0 AS cancelled_order_count,
        0 AS cancelled_order_amount,
        0 AS cancelled_order_apd,
        0 AS shipped_order_count,
        0 AS shipped_order_amount,
        0 AS shipped_order_apd,
        0 AS payment_apd,
        0 AS payment_amount,
        1 AS new_customer_apd,
        0 AS new_customer_paid_apd
    FROM devdw.Customers c
    cross join {{ source('etl_metadata', 'batch_control')}} B
    WHERE c.src_create_timestamp >= B.etl_batch_date
)
SELECT C.summary_date,
    C.dw_customer_id,
    MAX(C.order_count) AS order_count,
    MAX(C.order_apd) AS order_apd,
    MAX(C.order_amount) AS order_amount,
    MAX(C.order_cost_amount) AS order_cost_amount,
    MAX(C.order_mrp_amount) AS order_mrp_amount,
    MAX(C.products_ordered_qty) AS products_ordered_qty,
    MAX(C.products_items_qty) AS products_items_qty,
    MAX(C.cancelled_order_count) AS cancelled_order_count,
    MAX(C.cancelled_order_amount) AS cancelled_order_amount,
    MAX(C.cancelled_order_apd) AS cancelled_order_apd,
    MAX(C.shipped_order_count) AS shipped_order_count,
    MAX(C.shipped_order_amount) AS shipped_order_amount,
    MAX(C.shipped_order_apd) AS shipped_order_apd,
    MAX(C.payment_apd) AS payment_apd,
    MAX(C.payment_amount) AS payment_amount,
    MAX(C.new_customer_apd) AS new_customer_apd,
    MAX(C.new_customer_paid_apd) AS new_customer_paid_apd,
    CURRENT_TIMESTAMP AS create_timestamp,
    B.etl_batch_no AS etl_batch_no,  -- etl_batch_no parameter
    B.etl_batch_date AS etl_batch_date  -- etl_batch_date parameter
FROM CTE C 
cross join {{ source('etl_metadata', 'batch_control')}} B
GROUP BY C.summary_date,
        C.dw_customer_id;
