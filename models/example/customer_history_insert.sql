{{ config(
    materialized='incremental',
    unique_key='dw_customer_id'
) }}

with new_customers as (
    select
        C.dw_customer_id,
        C.creditLimit,
        {{ source('etl_metadata', 'batch_control') }}.etl_batch_no,
        {{ source('etl_metadata', 'batch_control') }}.etl_batch_date
    from
        {{ ref('customers') }} C
    left join {{ this }} ch on C.dw_customer_id = ch.dw_customer_id
    where ch.dw_customer_id is null or ch.dw_active_record_ind = 1
)

select
    dw_customer_id,
    creditLimit,
    {{ source('etl_metadata', 'batch_control') }}.etl_batch_date as effective_from_date,
    null as effective_to_date,
    1 as dw_active_record_ind,
    {{ source('etl_metadata', 'batch_control') }}.etl_batch_no as create_etl_batch_no,
    {{ source('etl_metadata', 'batch_control') }}.etl_batch_date as create_etl_batch_date,
    null as update_etl_batch_no,
    null as update_etl_batch_date,
    null as dw_update_timestamp
from
    new_customers