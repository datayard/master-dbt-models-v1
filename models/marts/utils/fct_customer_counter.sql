
SELECT
    'commercial_accounts' as accounttype,
    COUNT (distinct account.accountid) as accounts
FROM
    {{ ref('stg_salesforce_account') }} as account
    --dbt_vidyard_master.stg_salesforce_account as account
WHERE
    account.accounttype LIKE 'Customer'
    AND account.employeesegment LIKE 'Commercial'
    AND account.ispersonaccount = false
UNION

SELECT
    'emerging_accounts' as accounttype,
    COUNT (distinct account.accountid) as accounts
FROM
    {{ ref('stg_salesforce_account') }} as account
    --dbt_vidyard_master.stg_salesforce_account as account
WHERE
    account.accounttype LIKE 'Customer'
    AND account.employeesegment NOT LIKE 'Commercial'
    AND account.ispersonaccount = false
UNION

select case
        when z.sku = 'SS-010' then 'pro_self_serve'
        when z.sku = 'SKU-00000009' or z.sku = 'SKU-00000020' then 'enterprise_self_serve'
       end as accounttype,
       count(distinct z.accountid)
-- from dbt_vidyard_master.kube_zuora_ss_subscriptions z
from {{ ref('kube_zuora_ss_subscriptions') }} z
-- left join dbt_vidyard_master.tier2_vidyard_user_details u on u.organizationid = z.vidyardaccountid
left join {{ ref('tier2_vidyard_user_details') }} u on u.organizationid = z.vidyardaccountid
where z.promocodebillthrough
and z.trailbillthrough
and z.trailbillthroughnextday
and z.sublongerthenoneday
-- and u.classification != 'enterprise user'
and (z.subscriptionenddate is null or z.subscriptionenddate > getdate())
group by 1