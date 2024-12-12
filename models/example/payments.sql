{{ config(
    materialized='incremental',
    unique_key=['checkNumber', 'src_customernumber']
) }}

with ranked_data as (
    select
        c.dw_customer_id,
        sd.customernumber as src_customernumber,
        sd.checknumber as checknumber,
        sd.paymentdate,
        sd.amount,
        sd.create_timestamp as src_create_timestamp,
        coalesce(sd.update_timestamp, ed.src_update_timestamp) as src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        current_timestamp as dw_update_timestamp,
        case
            when ed.checknumber is null and ed.src_customernumber is null then current_timestamp
            else ed.dw_create_timestamp
        end as dw_create_timestamp,
        coalesce(ed.dw_payment_id,row_number() over (order by sd.checknumber) + coalesce(max(ed.dw_payment_id) over (), 0)) as dw_payment_id
    from
        {{ source('devstage', 'Payments')}} sd
    left join {{ this }} ed on sd.checknumber = ed.checknumber
    join {{ ref('customers') }} c on sd.customernumber = c.src_customernumber
    cross join {{ source('etl_metadata', 'batch_control')}} em
)

select *
from ranked_data
