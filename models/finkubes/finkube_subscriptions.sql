with sfdc as
(
  select * from {{ref('fintier2_sfdc_subscriptions')}}
)
, zuora as
(
  select * from {{ref('fintier2_zuora_subscriptions')}}
)

select
  accountid
  , region
--  , idtype
  , yearmonth
  , yearmonthvalue
  , fiscalyearmonth
  , fiscalquarter
  , fiscalyear
  , case when a.ispersonaccount or a.isselfserve or a.ispersonaccount is null then 'Vidyard Pro' else a.employeesegment end as customertype
  , case when yearmonth <= '2020-11' then nvl(sarr,0) else nvl(zarr,0) end as arr
   , case when yearmonth <= '2020-11' then nvl(sarr/12,0) else nvl(zarr/12,0) end as mrr
from zuora
full outer join sfdc using (accountid, region, idtype, yearmonth, yearmonthvalue, fiscalyearmonth, fiscalquarter, fiscalyear)
left join {{ref('tier2_salesforce_account')}} a using (accountid)
