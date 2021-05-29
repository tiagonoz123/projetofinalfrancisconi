with source_data as (

    select 
    salesreasonid,
    name,
	modifieddate,
	reasontype
    from {{ source('adventureworkstiago','salesreason') }}

)

select *
from source_data