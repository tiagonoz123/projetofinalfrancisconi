with source_data as (

    select 
    purchaseordernumber,
    shipmethodid,
    salesorderid,
    billtoaddressid,
    salespersonid,
    modifieddate,
    rowguid,
    taxamt,
    shiptoaddressid,
    onlineorderflag,
    territoryid,
    status,
    currencyrateid,
    orderdate,
    creditcardapprovalcode,
    subtotal,
    creditcardid,
    revisionnumber,
    freight,
    duedate,
    totaldue,
    customerid,
    shipdate,
    accountnumber
    from {{ source('adventureworkstiago','salesorderheader') }}

)

select *
from source_data