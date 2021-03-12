
    
    



select count(*) as validation_errors
from "dev"."dbt_vidyard_master"."stg_salesforce_accounts"
where account_id is null


