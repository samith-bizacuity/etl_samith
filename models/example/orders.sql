{{ config(
    materialized='incremental',
    unique_key='src_orderNumber'
) }}

with ranked_data as (
    select
        sd.ordernumber as src_orderNumber,
        sd.orderdate,
        sd.requireddate,
        sd.shippeddate,
        sd.status,
        sd.customernumber as src_customernumber,
        sd.cancelleddate,
        sd.create_timestamp as src_create_timestamp,
        coalesce(sd.update_timestamp, ed.src_update_timestamp) as src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        current_timestamp as dw_update_timestamp,
        case
            when ed.src_ordernumber is not null then current_timestamp
            else ed.dw_create_timestamp
        end as dw_create_timestamp,
        row_number() over (order by sd.ordernumber) + coalesce(max(ed.dw_order_id) over (), 0) as dw_order_id,
        coalesce(ed.dw_customer_id, c.dw_customer_id) as dw_customer_id
    from
        {{ source('devstage', 'Orders') }} sd
    left join {{ this }} ed on sd.ordernumber = ed.src_ordernumber
    cross join {{ source('etl_metadata', 'batch_control') }} em
    left join {{ ref('customers') }} c ON sd.customerNumber = c.src_customerNumber
)

select *
from ranked_data
