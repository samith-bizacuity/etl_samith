{{ config(
    materialized='incremental',
    unique_key='employeenumber'
) }}

with ranked_data as (
    select
        sd.employeenumber,
        sd.lastname,
        sd.firstname,
        sd.extension,
        sd.email,
        sd.officecode,
        sd.reportsto,
        sd.jobtitle,
        o.dw_office_id,
        sd.create_timestamp as src_create_timestamp,
        coalesce(sd.update_timestamp, ed.src_update_timestamp) as src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        current_timestamp as dw_update_timestamp,
        case
            when ed.employeenumber is null then current_timestamp
            else ed.dw_create_timestamp
        end as dw_create_timestamp,
        row_number() over (order by sd.employeenumber) + coalesce(max(ed.dw_employee_id) over (), 0) as dw_employee_id,
        0 as dw_reporting_employee_id
    from
        {{ source('devstage', 'Employees')}} sd
    left join {{ this }} ed on sd.employeenumber = ed.employeenumber
    join {{ ref('offices') }} o on sd.officecode = o.officecode
    cross join {{ source('etl_metadata', 'batch_control')}} em
),
updated_reporting as (
    select
        ranked_data.employeenumber,
        dw2.dw_employee_id as dw_reporting_employee_id
    from
        ranked_data
    join {{ this }} dw2 on ranked_data.reportsto = dw2.employeenumber
)

select
    rd.employeenumber,
    rd.lastname,
    rd.firstname,
    rd.extension,
    rd.email,
    rd.officecode,
    rd.reportsto,
    rd.jobtitle,
    rd.dw_office_id,
    rd.src_create_timestamp,
    rd.src_update_timestamp,
    rd.etl_batch_no,
    rd.etl_batch_date,
    rd.dw_create_timestamp,
    rd.dw_update_timestamp,
    rd.dw_employee_id,
    ur.dw_reporting_employee_id, rd.dw_reporting_employee_id as dw_reporting_employee_id
from
    ranked_data rd
left join updated_reporting ur on rd.employeenumber = ur.employeenumber
