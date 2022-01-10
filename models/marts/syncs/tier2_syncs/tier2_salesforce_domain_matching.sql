SELECT
    regexp_substr(userdomains.domain,'[^.]*') as simplyfieddomain,
    sfdcdomains.emaildomain,
    sfdcdomains.accountid
FROM
    {{ ref('tier2_users_per_domain') }} userdomains
    --dbt_vidyard_master.tier2_users_per_domain as userdomains
LEFT JOIN
    {{ ref('tier2_salesforce_account') }} sfdcdomains
    --dbt_vidyard_master.tier2_salesforce_account as sfdcdomains
ON
    regexp_substr(userdomains.domain,'[^.]*') = regexp_substr(sfdcdomains.emaildomain,'[^.]*')
WHERE
    true
    AND len(regexp_substr(userdomains.domain,'[^.]*')) > 2
    AND sfdcdomains.ispersonaccount = false