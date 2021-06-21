with
     source as (
         select 
         productcategoryid	
         ,name as prodcategory_name
         ,modifieddate as prodcategory_modifieddate 
         ,rowguid as prodcategory_rowguid
         ,_sdc_table_version		
         ,_sdc_received_at
         ,_sdc_extracted_at
         ,_sdc_sequence	
         ,_sdc_batched_at as last_etl_run

     from {{ source('adventureworkstiago','productcategory') }}
 )
 select * from source 