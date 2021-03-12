with source as (

    select 
    	sfdc_account.id,
    	sfdc_account.name
<<<<<<< HEAD
    from {{ source('salesforce', 'account') }} as sfdc_account
=======
    from {{ source('salesforce', 'account') }}
>>>>>>> c6eb2ebe6072f7ea7a94a263a7a462b6c5f0ce48
        where true

),

renamed as (

    select
        id as account_id,
        name as name

    from source

)

select 
	*
from renamed