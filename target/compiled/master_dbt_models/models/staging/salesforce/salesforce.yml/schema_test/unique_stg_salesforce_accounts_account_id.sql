
    
    



select count(*) as validation_errors
from (

    select
        account_id

    from "dev"."dbt_vidyard_master"."stg_salesforce_accounts"
    where account_id is not null
    group by account_id
    having count(*) > 1

) validation_errors


