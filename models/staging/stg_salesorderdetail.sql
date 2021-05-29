with source_data as (

    select 
    orderqty,
	salesorderid,
	salesorderdetailid,
	unitprice,
    specialofferid,
	modifieddate,
	rowguid,
	productid,
	carriertrackingnumber,
	unitpricediscount
    from {{ source('adventureworkstiago','salesorderdetail') }}

)

select *
from source_data