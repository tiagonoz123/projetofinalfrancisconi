with salesorder as(
    select *
    from {{ ref('stg_salesorderheader')}}
     ),
     salesdetails as(
    select *
    from {{ ref('stg_salesorderdetail')}}
     ),
     salesreason as(
    select *
    from {{ ref('stg_salesreason')}}
     ),
     salesheaderreason as(
    select *
    from {{ ref('stg_salesorderheadersalesreason')}}
     ),
     creditcard as(
    select *
    from {{ ref('stg_creditcard')}}
     ),
      customer as(
    select *
    from {{ ref('stg_customer')}}
     ),
      store as(
    select *
    from {{ ref('stg_store')}}
     ),
      salesterritory as(
    select *
    from {{ ref('stg_salesterritory')}}
     ),

     staging as (
    select
    salesorder.salesorderid,
    salesorder.salespersonid,
    salesorder.territoryid,
    salesorder.orderdate,
    salesorder.subtotal,
    salesorder.creditcardid,
    salesorder.status,
    store.businessentityid,
    salesdetails.orderqty,
    salesdetails.unitprice,
    salesdetails.productid,
    creditcard.cardtype,
    salesterritory.name
    from salesorder
    left join salesdetails on salesorder.salesorderid = salesdetails.salesorderid
    left join creditcard on salesorder.creditcardid = creditcard.creditcardid
    left join salesterritory on salesorder.territoryid = salesterritory.territoryid
    left join store on salesorder.salespersonid = store.salespersonid
  
    
     ),

 final as (
     select
     row_number() over (order by salesorderid) as sales_sk, --chave surrogate auto incremental
     *
     from staging
 )

 select * from final