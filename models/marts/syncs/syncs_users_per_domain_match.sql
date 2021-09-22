WITH maus as (
    SELECT
        COUNT (distinct mau.userid) as maus,
        mau.domain
    FROM
        dbt_vidyard_master.tier2_mau as mau
    WHERE
        mau = 1
    GROUP BY
        mau.domain
)

SELECT
    sfdcAccount.accountid,
    sfdcAccount.accountname,
    sfdcAccount.accounttype,
    usrdetails.domain,
    maus.maus,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'free' then 1 end) as free,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'pro' then 1 end) as pro,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'enterprise' then 1 end) as enterprise,
    COUNT (CASE WHEN usrdetails.personal_account_type like 'enterprise self serve' then 1 end) as enterprise_self_serve
FROM
    {{ ref('tier2_vidyard_user_details') }} usrdetails
    --dbt_vidyard_master.tier2_vidyard_user_details as usrdetails
LEFT JOIN
    maus
ON
    maus.domain = usrdetails.domain
LEFT JOIN
    {{ ref('tier2_salesforce_account') }} sfdcAccount
    --dbt_vidyard_master.tier2_salesforce_account as sfdcAccount
ON
    usrdetails.domain = sfdcAccount.emaildomain
WHERE
    usrdetails.domaintype like 'business'
GROUP BY
    sfdcAccount.accountid,
    sfdcAccount.accountname,
    sfdcAccount.accounttype,
    usrdetails.domain,
    maus.maus
ORDER BY
    usrdetails.domain ASC



