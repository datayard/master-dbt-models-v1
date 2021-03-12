

  create view "dev"."dbt_vidyard_master"."stg_salesforce_accounts__dbt_tmp" as (
    with source as (

    select 
    	sfdc_account.id,
    	sfdc_account.name

    from "dev"."salesforce_production"."account" as sfdc_account

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
  ) ;
