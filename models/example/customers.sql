{{ config(
    materialized='incremental',
    unique_key='src_customerNumber'
) }}

with max_dw_id as (
    SELECT max(dw_customer_id) AS mid
    FROM {{ this }}
),
ranked_data as (
    select
        sd.customernumber as src_customernumber,
        sd.customername,
        sd.contactlastname,
        sd.contactfirstname,
        sd.phone,
        sd.addressline1,
        sd.addressline2,
        sd.city,
        sd.state,
        sd.postalcode,
        sd.country,
        sd.salesrepemployeenumber,
        sd.creditlimit,
        e.dw_employee_id as dw_sales_employee_id,
        sd.create_timestamp as src_create_timestamp,
        coalesce(sd.update_timestamp, ed.src_update_timestamp) as src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        current_timestamp as dw_update_timestamp,
        case
            when ed.src_customernumber is null then current_timestamp
            else ed.dw_create_timestamp
        end as dw_create_timestamp,
        coalesce(ed.dw_customer_id, row_number() over (order by sd.customernumber) + coalesce(max_dw_id.mid, 0)) as dw_customer_id
    from
        {{ source('devstage', 'Customers')}} sd
    left join {{ this }} ed on sd.customernumber = ed.src_customernumber
    left join {{ ref('employees') }} e on sd.salesrepemployeenumber = e.employeenumber
    cross join {{ source('etl_metadata', 'batch_control')}} em
    cross join max_dw_id
)

select *
from ranked_data