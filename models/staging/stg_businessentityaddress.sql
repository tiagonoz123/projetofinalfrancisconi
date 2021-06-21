with
     source as (
         select 
         businessentityid
         ,addressid
        
         ,addresstypeid	


         ,modifieddate	
         ,rowguid			
         	

     
         ,_sdc_table_version		
         ,_sdc_received_at
         ,_sdc_extracted_at
         ,_sdc_sequence	
         ,_sdc_batched_at as last_etl_run

     from {{ source('adventureworkstiago','businessentityaddress') }}
     ),
       
       
     k as (
         select source.businessentityid,
         case when  source.addresstypeid = 5 
         then source.addressid else null end as shipping_address
         from source
     ),
     kk as (
         select k.businessentityid, k.shipping_address
         from k
         where k.shipping_address IS NOT null
     ),
     
     x as (
         select source.businessentityid,
         case when source.addresstypeid = 2 or source.addresstypeid = 3 or 
         source.addresstypeid = 4 or source.addresstypeid = 1
         then source.addressid else null end as home_address
         from source
     ),
     xx as (
         select x.businessentityid, x.home_address
         from x
         where x.home_address IS NOT null
     ),
     
     final as (
         select  
         distinct source.businessentityid
 
         ,kk.shipping_address 
         ,xx.home_address
	
         
         from source
         left join xx on source.businessentityid = xx.businessentityid
         left join kk on source.businessentityid = kk.businessentityid
     )
     
     select *
     from final