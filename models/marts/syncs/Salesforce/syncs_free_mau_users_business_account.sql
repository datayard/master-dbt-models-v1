SELECT
 count(distinct mau.userid) as free_maus,
 sfdcaccount.vidyardaccountid,
 sfdcaccount.accountname,
 sfdcaccount.accountid
FROM

{{ref ('tier2_mau')}} as mau
 --dbt_vidyard_master.tier2_mau as mau
JOIN
{{ref ('tier2_salesforce_account')}} as sfdcaccount
 --dbt_vidyard_master.tier2_salesforce_account as sfdcaccount
ON
 sfdcaccount.emaildomain = mau.domain
WHERE
 mau = 1 and
 sfdcaccount.ispersonaccount = false and
 mau.personal_account_type = 'free'
 AND mau.domaintype = 'business'
 AND sfdcaccount.vidyardaccountid is NOT NULL
GROUP BY
 sfdcaccount.vidyardaccountid,
 sfdcaccount.accountname,
 sfdcaccount.accountid