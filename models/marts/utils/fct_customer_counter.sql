SELECT
    'Enterprise_users' as accounttype,
    COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE( vidyard_allotments.allotmentlimit  ,0)*(1000000*1.0)) AS DECIMAL(38,0))) + CAST(STRTOL(LEFT(MD5(CAST( vidyard_allotments.accountid  AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST( vidyard_allotments.accountid  AS VARCHAR)),15),16) AS DECIMAL(38,0)) ) - SUM(DISTINCT CAST(STRTOL(LEFT(MD5(CAST( vidyard_allotments.accountid  AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST( vidyard_allotments.accountid  AS VARCHAR)),15),16) AS DECIMAL(38,0))) )  AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) AS "Enterprise"
FROM
    dbt_vidyard_master.kube_org_wide_metrics  AS kube_org_wide_metrics
INNER JOIN
    dbt_vidyard_master.tier2_vidyard_allotments  AS vidyard_allotments
ON
    kube_org_wide_metrics.accountid = vidyard_allotments.accountid
INNER JOIN
    dbt_vidyard_master.tier2_salesforce_account  AS tier2_salesforce_account
ON
    kube_org_wide_metrics.accountid = tier2_salesforce_account.vidyardaccountid
    and tier2_salesforce_account.ispersonaccount=False
WHERE
    (vidyard_allotments.name ) = 'govideo_users'
    AND (tier2_salesforce_account.accounttype ) IN ('Customer', 'Sub-Account')
    AND ( (TO_CHAR(DATE_TRUNC('month', DATE_TRUNC('quarter', DATEADD(month,8, DATE_TRUNC('month', tier2_salesforce_account.originalcontractdate ) ))), 'YYYY-MM'))
          ) IS NOT NULL
UNION

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
       /*case 
        when z.sku = 'SS-010' OR z.sku = 'SKU-00000009' or z.sku = 'SKU-00000020' then 'pro_self_serve'
        end as accounttypeII,*/ -- Use this only in case we need an aggregation of all user types in pro
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
and (z.subscriptionenddate is null or z.subscriptionenddate >= (DATE(DATEADD(minute,0, DATE_TRUNC('minute', GETDATE())))))
group by 1

