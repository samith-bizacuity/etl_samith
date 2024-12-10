{{ config(
    materialized='incremental',
    unique_key=['src_orderNumber, src_productCode']
) }}

with ranked_data as (
    select
        sd.ordernumber as src_orderNumber,
        sd.productcode as src_productCode,
        sd.quantityordered,
        sd.priceeach,
        sd.orderlinenumber,
        sd.create_timestamp as src_create_timestamp,
        coalesce(sd.update_timestamp, ed.src_update_timestamp) as src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        current_timestamp as dw_update_timestamp,
        case
            when ed.src_orderNumber is null and ed.src_productCode is null then current_timestamp
            else ed.dw_create_timestamp
        end as dw_create_timestamp,
        row_number() over (order by sd.ordernumber, sd.productcode) + coalesce(max(ed.dw_orderdetail_id) over (), 0) as dw_orderdetail_id,
        coalesce(o.dw_order_id, ed.dw_order_id) as dw_order_id,
        coalesce(p.dw_product_id, ed.dw_product_id) as dw_product_id 
    from
        {{ source('devstage', 'OrderDetails') }} sd
    left join {{ this }} ed on sd.ordernumber = ed.src_orderNumber and sd.productcode = ed.src_productCode
    left join {{ ref('products') }} p on sd.productcode = p.src_productcode
    left join {{ ref('orders') }} o on sd.ordernumber = o.src_ordernumber
    cross join {{ source('etl_metadata', 'batch_control') }} em
)

select *
from ranked_data
