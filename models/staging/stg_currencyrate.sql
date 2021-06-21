with
     source as (
         select 
         currencyrateid
         ,fromcurrencycode
         ,endofdayrate	
         ,tocurrencycode	
         ,modifieddate		
         ,currencyratedate		
         ,averagerate

     from {{ source('adventureworkstiago','currencyrate') }}
 )
 select * from source 