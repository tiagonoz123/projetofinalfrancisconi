with
     source as (
         select 
         specialofferid       
         ,startdate	
         ,maxqty	
         ,modifieddate	
         ,rowguid	
         ,type	
         ,discountpct	
         ,category		
         ,description	
         ,minqty	
         ,enddate	

     from {{ source('adventureworkstiago','specialoffer') }}
 )
 select * from source 