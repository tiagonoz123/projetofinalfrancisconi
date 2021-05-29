with source_data as (

    select 
    sellenddate,
    safetystocklevel,
	finishedgoodsflag,
	class,
	makeflag,
	productnumber,
	reorderpoint,
	modifieddate,
	rowguid,
	productmodelid,
	weightunitmeasurecode,
	standardcost,
	name,
	style,
	sizeunitmeasurecode,
	productid,
    productsubcategoryid,
	listprice,
	daystomanufacture,
	productline,
	size,
	color,
	sellstartdate,
    weight
    from {{ source('adventureworkstiago','product') }}

)

select *
from source_data