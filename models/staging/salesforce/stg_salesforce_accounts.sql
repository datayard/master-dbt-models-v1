with source as (

    select 
    	sfdc_account.id,
    	sfdc_account.name

    from {{ source('salesforce', 'account') }} as sfdc_account

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