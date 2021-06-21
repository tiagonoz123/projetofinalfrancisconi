with source_data as (

    select *
    from {{ source('adventureworkstiago','address') }}

)

select *
from source_data