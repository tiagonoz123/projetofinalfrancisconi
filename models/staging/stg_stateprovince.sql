with
     source as (
         select 
         stateprovinceid 
        
         ,territoryid
         ,countryregioncode
    
         ,name as stateprovince_name	
         ,stateprovincecode
         ,modifieddate	
         ,rowguid				
         ,isonlystateprovinceflag		
         ,_sdc_table_version		
         ,_sdc_received_at
         ,_sdc_extracted_at
         ,_sdc_sequence	
         ,_sdc_batched_at as last_etl_run

     from {{ source('adventureworkstiago','stateprovince') }}
 )
 select * from source 