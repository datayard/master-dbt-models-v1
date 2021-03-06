with dates_table as (
  select
    yearmonthvalue
    , yearmonth
    , fiscalyearmonth
    , fiscalquarter
    , fiscalyear
   from {{ ref('fct_sfdc_dates') }}
   group by 1,2,3,4,5
 )

 , sfdc as (
   select
     accountid
     , region
     , case when a.ispersonaccount then 'Vidyard Pro' when a.ispersonaccount is null then 'Vidyard Pro' else a.employeesegment end as customertype
   from {{ref('tier2_salesforce_account')}} a
   left join {{ref('fct_sfdc_country_to_region')}} c on lower(a.billingcountry) = c.country
 )

, zuora as (
  select
  --  case when a.crmid is null then a.accountnumber else left(a.crmid, 15) end as accountid15
  case when p.sku in ('SKU-00000009','SKU-00000020','SS-010') then a.accountnumber else nvl(sfdc.accountid,nvl(a.crmid,a.accountnumber)) end as accountid
    , case when a.crmid is null then 'zid' else 'accountid' end as idtype
    , case when p.sku in ('SKU-00000009','SKU-00000020','SS-010') then 'Vidyard Pro' when sfdc.customertype in ('Emerging','Commercial') then sfdc.customertype else 'Emerging' end as customertype
    , to_char(date_trunc('month', (case when s.serviceactivationdate > rpc.effectivestartdate then s.serviceactivationdate else rpc.effectivestartdate end)), 'yyyy-mm') as startyearmonth
    , case when date_part('day',rpc.effectiveenddate) >= 28
        and date_part('day',case when s.serviceactivationdate > rpc.effectivestartdate then s.serviceactivationdate else rpc.effectivestartdate end) = 1
        then to_char(date_trunc('month', rpc.effectiveenddate), 'yyyy-mm')
        else to_char(date_trunc('month', rpc.effectiveenddate), 'yyyy-mm') end as endyearmonth
        , sfdc.region
    , sum(nvl(rpc.mrr * 12, 0)) as arr
  FROM {{ ref('stg_zuora_rate_plan') }} AS rp
    JOIN {{ ref('stg_zuora_subscription') }} AS s ON s.subscriptionid = rp.subscriptionid
    JOIN {{ ref('stg_zuora_rate_plan_charge')}} AS rpc ON rpc.rateplanid = rp.rateplanid
    JOIN {{ ref('stg_zuora_account') }} AS a ON a.accountid = s.accountid
    JOIN {{ ref('stg_zuora_product_rate_plan') }} AS prp ON prp.productrateplanid = rp.productrateplanid
    JOIN {{ ref('stg_zuora_product') }} AS p ON p.productid = prp.productid
    JOIN {{ ref('stg_zuora_contact') }} c ON c.contactid = s.soldtocontactid
    left join sfdc on left(sfdc.accountid,15) = left(a.crmid,15)
  where
    s.fivetrandeleted = 'f'
    and s.status <> 'Expired'
    and p.name <> 'Open Balance'
    and nvl(rpc.mrr, 0) <> 0
    and (termenddate > serviceactivationdate or termenddate is null)
  --  and (amendmenttype <> 'RemoveProduct' or amendmenttype is null)
  group by 1,2,3,4,5,6

)

, zuora_discount as (
  select
  --  case when a.crmid is null then a.accountnumber else left(a.crmid, 15) end as accountid15
  case when p.sku in ('SKU-00000009','SKU-00000020','SS-010') then a.accountnumber else nvl(sfdc.accountid,nvl(a.crmid,a.accountnumber)) end as accountid
    , to_char(date_trunc('month', rpc.effectivestartdate), 'yyyy-mm') as startyearmonth
    , case when date_part('day',rpc.effectiveenddate) >= 28 and date_part('day',rpc.effectivestartdate) = 1
        then to_char(date_trunc('month', rpc.effectiveenddate+4), 'yyyy-mm')
        else to_char(date_trunc('month', rpc.effectiveenddate), 'yyyy-mm') end as endyearmonth
    --, case when lower(rp.name) in ('three months free','first month free','12 month discount','free forever','first month free - advisor group','100% annual discount')
    --    then 1.0 when lower(rp.name) in ('aflac annual','annual') then 0.5 else 1.0 end as discountpercent
    , rpct.discountpercentage * 1.0 / 100 as discountpercent
  FROM {{ ref('stg_zuora_rate_plan') }} AS rp
    JOIN {{ ref('stg_zuora_subscription') }} AS s ON s.subscriptionid = rp.subscriptionid
    JOIN {{ ref('stg_zuora_rate_plan_charge')}} AS rpc ON rpc.rateplanid = rp.rateplanid
    JOIN {{ ref('stg_zuora_rate_plan_charge_tier')}} as rpct ON rpct.rateplanchargeid = rpc.rateplanchargeid
    JOIN {{ ref('stg_zuora_account') }} AS a ON a.accountid = s.accountid
    JOIN {{ ref('stg_zuora_product_rate_plan') }} AS prp ON prp.productrateplanid = rp.productrateplanid
    JOIN {{ ref('stg_zuora_product') }} AS p ON p.productid = prp.productid
    JOIN {{ ref('stg_zuora_contact') }} c ON c.contactid = s.soldtocontactid
    left join sfdc on left(sfdc.accountid,15) = left(a.crmid,15)
  where
    s.fivetrandeleted = 'f'
    and s.status <> 'Expired'
    and p.name <> 'Open Balance'
    and rpc.chargemodel = 'Discount-Percentage'
  group by
    1,2,3,4
)



--REPLACE ZUORA_PRO with ZUORA TABLES NOT ZUORA SUBSCRIPTION__C
, zuora_sfdc_id_adjust as (
  select
    z.accountid
    , z.region
    , z.idtype
    , z.customertype
    , z.startyearmonth
    , z.endyearmonth
    , z.arr
  from zuora z
--  left join sfdc a on left(a.accountid,15) = z.accountid15
)

, zuora_discount_sfdc_id_adjust as (
  select
    z.accountid
    , z.startyearmonth
    , z.endyearmonth
    , z.discountpercent
  from zuora_discount z
--  left join sfdc a on left(a.accountid,15) = z.accountid15
)

, zuora_pre_discount as (
  select
    z.accountid
    , z.region
    , z.idtype
    , z.customertype
    , d.yearmonth
    , d.yearmonthvalue
    , d.fiscalyearmonth
    , d.fiscalquarter
    , d.fiscalyear
    , sum(arr) as zzarr
  from zuora_sfdc_id_adjust z
    left join dates_table d on
      d.yearmonth >= z.startyearmonth
      and (d.yearmonth < z.endyearmonth or z.endyearmonth is null)
  where
    arr > 0
    and d.yearmonth is not null
  group by 1,2,3,4,5,6,7,8,9
)

, zuora_discount_final as (
  select
    z.accountid
    , d.yearmonth
    , discountpercent
  from zuora_discount_sfdc_id_adjust z
  left join dates_table d on
    d.yearmonth >= z.startyearmonth
    and (d.yearmonth < z.endyearmonth or z.endyearmonth is null)
  where
    d.yearmonth is not null
)

select
  accountid
  , region
  , idtype
  , customertype
  , yearmonth
  , yearmonthvalue
  , fiscalyearmonth
  , fiscalquarter
  , fiscalyear
  , nvl(zzarr,0) - nvl(zzarr,0) * nvl(discountpercent,0) as zarr
from zuora_pre_discount
left join zuora_discount_final using (accountid, yearmonth)
