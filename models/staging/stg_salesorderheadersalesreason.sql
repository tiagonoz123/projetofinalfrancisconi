with
     source as (
         select 
         salesreasonid 
         ,salesorderid	
         
         ,modifieddate	
         ,_sdc_table_version		
         ,_sdc_received_at
         ,_sdc_extracted_at
         ,_sdc_sequence	
         ,_sdc_batched_at as last_etl_run

     from {{ source('adventureworkstiago','salesorderheadersalesreason') }}
     )
         , keyssalesreason as (
         select salesorderid, salesreasonid
         ,row_number () over (partition by salesorderid) as rownumber
         from source
         )

         , n_rep as (
         select salesorderid, salesreasonid as motivo_1
         from keyssalesreason
         where rownumber = 1)

         , rep_um as (
         select salesorderid, salesreasonid as motivo_2
         from keyssalesreason
         where rownumber = 2)

         , rep_dois as (
         select salesorderid, salesreasonid as motivo_3
         from keyssalesreason
         where rownumber = 3)

         , final as (
         select n_rep.salesorderid
         , n_rep.motivo_1
         , rep_um.motivo_2
         , rep_dois.motivo_3
         from n_rep
         left join rep_um on n_rep.salesorderid = rep_um.salesorderid
         left join rep_dois on n_rep.salesorderid = rep_dois.salesorderid
         )


 select * from final 