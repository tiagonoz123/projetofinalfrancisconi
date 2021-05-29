with source_data as (

    select 
    nationalidnumber,
    sickleavehours,
	loginid,
	currentflag,
	modifieddate,
	rowguid,
	gender,
	hiredate,
	salariedflag,
	birthdate,
	maritalstatus,
	organizationnode,
	businessentityid,
	vacationhours,
	jobtitle
    from {{ source('adventureworkstiago','employee') }}

)

select *
from source_data