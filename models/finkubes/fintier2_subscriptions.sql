with sfdc_raw as
(
  select * from {{ref('fintier2_sfdc_subscriptions')}}
)
, zuora as
(
  select * from {{ref('fintier2_zuora_subscriptions')}}
)

, sfdc2 as (
  select accountid as sfaccountid, left(accountid,15) as sfaccountid15, case when a.ispersonaccount then 'Vidyard Pro' when a.employeesegment = 'HubSpot Self Serve' then 'Emerging' else a.employeesegment end as customertype  from {{ref('tier2_salesforce_account')}} a
)

, zidandtype as (
  select
    case when p.sku in ('SKU-00000009','SKU-00000020','SS-010') then a.accountnumber else nvl(sfdc2.sfaccountid,nvl(a.crmid,a.accountnumber)) end as accountid
    , case when p.sku in ('SKU-00000009','SKU-00000020','SS-010') then 'Vidyard Pro' when sfdc2.customertype in ('Emerging','Commercial') then sfdc2.customertype else 'Emerging' end as customertype
    , sfdc2.sfaccountid as sfaccountid
    , left(sfdc2.sfaccountid,15) as sfaccountid15
  FROM {{ ref('stg_zuora_rate_plan') }} AS rp
    JOIN {{ ref('stg_zuora_subscription') }} AS s ON s.subscriptionid = rp.subscriptionid
    JOIN {{ ref('stg_zuora_rate_plan_charge')}} AS rpc ON rpc.rateplanid = rp.rateplanid
    JOIN {{ ref('stg_zuora_account') }} AS a ON a.accountid = s.accountid
    JOIN {{ ref('stg_zuora_product_rate_plan') }} AS prp ON prp.productrateplanid = rp.productrateplanid
    JOIN {{ ref('stg_zuora_product') }} AS p ON p.productid = prp.productid
    JOIN {{ ref('stg_zuora_contact') }} c ON c.contactid = s.soldtocontactid
    left join sfdc2 on left(sfdc2.sfaccountid,15) = left(a.crmid,15)
  where
    s.fivetrandeleted = 'f'
    and s.status <> 'Expired'
    and p.name <> 'Open Balance'
    and nvl(rpc.mrr, 0) <> 0
    and (termenddate > serviceactivationdate or termenddate is null)
  --  and (amendmenttype <> 'RemoveProduct' or amendmenttype is null)
  group by 1,2,3

)

, sfdc as
(
  select
      nvl(z.accountid, r.accountid) as accountid
    , region, idtype
    , nvl(z.customertype, s.customertype) as customertype
   , yearmonth, yearmonthvalue, fiscalyearmonth, fiscalquarter, fiscalyear,sarr
  from sfdc_raw r
  left join sfdc2 s on s.sfaccountid = r.accountid
  left join zidandtype z on z.sfaccountid15 = s.sfaccountid15
)








, combined as (
select
  accountid
  , region
--  , idtype
, customertype
  , yearmonth
  , yearmonthvalue
  , fiscalyearmonth
  , fiscalquarter
  , fiscalyear

--  , case when yearmonth <= '2020-11' then nvl(sarr,0) else nvl(zarr,0) end as arr
--   , case when yearmonth <= '2020-11' then nvl(sarr/12,0) else nvl(zarr/12,0) end as mrr
--  , nvl(sarr,0) as salesforcearr
--  , nvl(sarr,0)/12 as salesforcemrr
--  , nvl(zarr,0) as zuoraarr
--  , nvl(zarr,0)/12 as zuoramrr

--  , case when sarr > 0 and yearmonth < '2020-12' then sarr when zarr > 0 then zarr else 0 end as arr
--  , case when sarr > 0 and yearmonth < '2020-12' then sarr/12 when zarr > 0 then zarr/12 else 0 end as mrr
    , sum(nvl(sarr,0)) as sarr
    , sum(nvl(zarr,0)) as zarr

from zuora
full outer join sfdc using (accountid, customertype, region, yearmonth, yearmonthvalue, fiscalyearmonth, fiscalquarter, fiscalyear)

group by 1,2,3,4,5,6,7,8
)



, chosen_arr as (
select accountid, region,yearmonth,yearmonthvalue,fiscalyearmonth,fiscalquarter,fiscalyear
, customertype
--, case when a.ispersonaccount or a.isselfserve or a.ispersonaccount is null then 'Vidyard Pro' else a.employeesegment end as customertype
, sum(case when sarr > 0 and yearmonth < '2020-12' then sarr when zarr > 0 then zarr else 0 end) as arr
 from combined c
 --left join sfdc2 a using (accountid)
 group by 1,2,3,4,5,6,7,8
)

select *, arr/12 as mrr
from chosen_arr
where arr <> 0
