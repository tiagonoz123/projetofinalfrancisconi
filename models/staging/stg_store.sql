with source_data as (

    select 
    salespersonid,
    modifieddate,
	rowguid,
	name,
    businessentityid
    from {{ source('adventureworkstiago','store') }}

)

select *
from source_data