with sfdc as
(
  select * from {{ref('fintier2_sfdc_subscriptions')}}
)
, zuora as
(
  select * from {{ref('fintier2_zuora_subscriptions')}}
)

, combined as (
select
  accountid
  , region
--  , idtype
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
full outer join sfdc using (accountid, region, idtype, yearmonth, yearmonthvalue, fiscalyearmonth, fiscalquarter, fiscalyear)

group by 1,2,3,4,5,6,7
)

, sfdc as (
  select lower(accountid) as accountid from {{ref('tier2_salesforce_account')}} a

)

, chosen_arr as (
select accountid, region,yearmonth,yearmonthvalue,fiscalyearmonth,fiscalquarter,fiscalyear
, case when a.ispersonaccount or a.isselfserve or a.ispersonaccount is null then 'Vidyard Pro' else a.employeesegment end as customertype
  , sum(case when sarr > 0 and yearmonth < '2020-12' then sarr when zarr > 0 then zarr else 0 end) as arr
  from combined c
  left join sfdc a using (accountid)
  group by 1,2,3,4,5,6,7,8
)

select *, arr/12 as mrr
from chosen_arr
where arr <> 0
