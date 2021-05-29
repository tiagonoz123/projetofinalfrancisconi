with source_data as (

    select 
    salesreasonid,
	salesorderid,
	modifieddate
    from {{ source('adventureworkstiago','salesorderheadersalesreason') }}

)

select *
from source_data