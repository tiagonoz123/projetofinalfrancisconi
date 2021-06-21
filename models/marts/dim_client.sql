with
 person as (
     select 
     /* Primary key */
     businessentityid
        
     /* Foreign keys */
         
     ,firstname
     ,middlename	
     ,lastname	
     ,persontype	
     ,namestyle	
     ,suffix	
     ,modifieddate	
     ,emailpromotion	
     ,title		


     from {{ ref('stg_person') }}
 )
, adresskey as (
   select
         businessentityid
        
         /* Foreign keys */
         ,shipping_address
         ,home_address

         

   from {{ ref('stg_businessentityaddress') }}
 )

 , address_home_address as (
   select
         addressid as home_addressid
        
         /* Foreign keys */
         ,stateprovinceid	as home_stateprovinceid
         
         ,city as home_city 	
         ,modifieddate as home_modifieddate 	
         ,rowguid as home_rowguid 	
         ,postalcode as home_postalcode 	
         ,spatiallocation as home_spatiallocation 	
         ,addressline1 as home_addressline1 	
         ,addressline2 as home_addressline2 		  
   
   from {{ ref('stg_address') }}
 )
 , stateprovince_home_address as (
   select
         /* Primary key */
         stateprovinceid as home_stateprovinceid
        
         /* Foreign keys */
         ,territoryid
         ,countryregioncode as home_countryregioncode
    
         ,stateprovince_name as	home_stateprovince_name
         ,stateprovincecode as home_stateprovincecode
         ,modifieddate as home_modifieddate
         ,rowguid as home_rowguid				
         ,isonlystateprovinceflag	as home_isonlystateprovinceflag 
   
   from {{ ref('stg_stateprovince') }}
 )
 , countryregion_home_address as (
   select        
         /* Primary key */
         countryregioncode
        
         /* Foreign keys */
         	
         
         ,countryregion_name as home_countryregion_name
         ,modifieddate
   
   from {{ ref('stg_countryregion') }}
 ) 
  , final as (
   select        
     /* Primary key */
     person.businessentityid
        
     /* Foreign keys */
         
     ,person.firstname
     ,person.middlename	
     ,person.lastname	
     ,person.persontype	
     ,person.namestyle	
     ,person.suffix	
     ,person.modifieddate	
     ,person.emailpromotion	
     ,person.title

     ,adresskey.shipping_address
     ,adresskey.home_address
        

      /* home_address */
     ,address_home_address.home_stateprovinceid	
     ,address_home_address.home_city	
     ,address_home_address.home_postalcode	
     ,address_home_address.home_spatiallocation	
     ,address_home_address.home_addressline1	
     ,address_home_address.home_addressline2	
         
     ,stateprovince_home_address.home_countryregioncode
     ,stateprovince_home_address.home_stateprovince_name		
     ,stateprovince_home_address.home_stateprovincecode
     ,stateprovince_home_address.home_isonlystateprovinceflag	

     ,countryregion_home_address.home_countryregion_name


   from person
   left join adresskey on person.businessentityid = adresskey.businessentityid

   
   /* home_address */
   left join address_home_address on adresskey.home_address = address_home_address.home_addressid
   left join stateprovince_home_address on address_home_address.home_stateprovinceid = stateprovince_home_address.home_stateprovinceid
   left join countryregion_home_address on stateprovince_home_address.home_countryregioncode = countryregion_home_address.countryregioncode

 ) 

 select * from final 