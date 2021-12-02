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
--  , case when yearmonth <= '2020-11' then nvl(sarr,0) else nvl(zarr,0) end as arr
--   , case when yearmonth <= '2020-11' then nvl(sarr/12,0) else nvl(zarr/12,0) end as mrr
--  , nvl(sarr,0) as salesforcearr
--  , nvl(sarr,0)/12 as salesforcemrr
--  , nvl(zarr,0) as zuoraarr
--  , nvl(zarr,0)/12 as zuoramrr
  , case when sarr > 0 and yearmonth < '2020-12' then sarr when zarr > 0 then zarr else 0 end as arr
  , case when sarr > 0 and yearmonth < '2020-12' then sarr/12 when zarr > 0 then zarr/12 else 0 end as mrr
from zuora
full outer join sfdc using (accountid, region, idtype, yearmonth, yearmonthvalue, fiscalyearmonth, fiscalquarter, fiscalyear)
left join {{ref('tier2_salesforce_account')}} a using (accountid)
