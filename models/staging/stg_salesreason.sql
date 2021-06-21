with
     source as (
         select 
         salesreasonid 
         ,name as salesreason_name		
         ,modifieddate		
         ,reasontype

     from {{ source('adventureworkstiago','salesreason') }}
 )
 select * from source 