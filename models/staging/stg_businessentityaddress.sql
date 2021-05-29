with source_data as (

    select *
    from {{ source('adventureworkstiago','businessentityaddress') }}

)

select *
from source_data