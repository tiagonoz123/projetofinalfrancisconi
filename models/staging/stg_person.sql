with source_data as (

    select 
	persontype,
	namestyle,
	suffix,
	modifieddate,
	emailpromotion,
	title,
	businessentityid,
	firstname,
	middlename,
    lastname
    from {{ source('adventureworkstiago','person') }}

)

select *
from source_data