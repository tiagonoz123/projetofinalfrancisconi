with
     source as (
         select 
         productsubcategoryid		
         ,productcategoryid	
         ,name as subcategory_name
         ,rowguid as subcategory_rowguid
         ,modifieddate as subcategory_modifieddate
         ,_sdc_table_version		
         ,_sdc_received_at
         ,_sdc_extracted_at
         ,_sdc_sequence	
         ,_sdc_batched_at as last_etl_run

     from {{ source('adventureworkstiago','productsubcategory') }}
 )
 select * from source 