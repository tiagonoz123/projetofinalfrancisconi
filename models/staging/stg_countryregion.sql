with
     source as (
         select 
         /* Primary key */
         countryregioncode 
        
         /* Foreign keys */
         	
         
         ,name as countryregion_name
         ,modifieddate	
         	

     
         /* Stich coluns */
         ,_sdc_table_version		
         ,_sdc_received_at
         ,_sdc_extracted_at
         ,_sdc_sequence	
         ,_sdc_batched_at as last_etl_run

     from {{ source('adventureworkstiago','countryregion') }}
 )
 select * from source 