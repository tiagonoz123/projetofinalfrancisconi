with source_data as (

    select 
    countryregioncode,
	modifieddate,
	rowguid,
	saleslastyear,
	name,
	territoryid,
	costytd,
	costlastyear,
	salesytd
    from {{ source('adventureworkstiago','salesterritory') }}

)

select *
from source_data