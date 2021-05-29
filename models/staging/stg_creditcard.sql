with source_data as (

    select 
    cardtype,
	expyear,
	modifieddate,
	expmonth,
	cardnumber,
	creditcardid
    from {{ source('adventureworkstiago','creditcard') }}

)

select *
from source_data