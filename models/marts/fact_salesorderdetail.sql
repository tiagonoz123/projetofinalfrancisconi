with sales as (
    select *
    from {{ ref('dim_sales')}}
),
    client as (
    select *
    from {{ ref('dim_client')}}
    ),
    
    final as (
    select 
    sales.salesorderid,
    sales.salespersonid,
    sales.territoryid,
    sales.orderdate,
    sales.subtotal,
    sales.creditcardid,
    sales.status,
    sales.orderqty,
    sales.unitprice,
    sales.productid,
    sales.cardtype,
    sales.name,
    client.shipping_address,
    client.home_address,
    client.home_stateprovinceid,
    client.home_city,
    client.home_postalcode,
    client.home_spatiallocation,
    client.home_addressline1,
    client.home_addressline2,
    client.home_countryregioncode,
    client.home_stateprovince_name,
    client.home_stateprovincecode,
	client.home_countryregion_name 
    from sales
    left join client on sales.businessentityid = client.businessentityid
    )

    select * from final
