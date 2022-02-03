SELECT
    usecase.accountid,
    account.accountname,
    usecase.usecase,
    account.primaryusecase,
    account.vidyardAccountId
FROM
    {{ ref('fct_sfdc_accounts_use_case') }} as usecase
    --dbt_vidyard_master.fct_sfdc_accounts_use_case as usecase
JOIN
     {{ ref('stg_salesforce_account') }} as account
    --dbt_vidyard_master.stg_salesforce_account as account
ON
    usecase.accountid = account.accountid
WHERE
    true
