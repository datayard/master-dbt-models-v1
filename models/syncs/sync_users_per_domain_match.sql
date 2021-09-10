SELECT
    sfdcaccount.accountid,
    sfdcaccount.accountname,
    usrdetails.domain,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'free' then 1 end) as free,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'pro' then 1 end) as pro,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'enterprise' then 1 end) as enterprise,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'enterprise self serve' then 1 end) as enterprise_self_serve
FROM
    {{ ref('tier2_vidyard_user_details') }} usrdetails
    --dbt_vidyard_master.tier2_vidyard_user_details as usrdetails
LEFT JOIN
    {{ ref('tier2_salesforce_account') }} sfdcaccount
    --dbt_vidyard_master.tier2_salesforce_account as sfdcaccount
ON
    usrdetails.domain LIKE sfdcaccount.emaildomain
WHERE
    usrdetails.domaintype like 'business'
    AND sfdcaccount.accountid IS null
GROUP BY
    sfdcaccount.accountid,
    sfdcaccount.accountname,
    usrdetails.domain
ORDER BY
    usrdetails.domain ASC