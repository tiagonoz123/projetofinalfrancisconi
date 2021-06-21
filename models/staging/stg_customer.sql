with source_data as (

    select 
    personid,
	modifieddate,
    rowguid,
    territoryid,
    storeid,
    customerid
    from {{ source('adventureworkstiago','customer') }}

)

select *
from source_data