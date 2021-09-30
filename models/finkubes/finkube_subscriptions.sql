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
--  , idtype
  , yearmonth
  , yearmonthvalue
  , fiscalyearmonth
  , fiscalperiod
  , fiscalyear
  , case when yearmonth <= '2020-11' then nvl(sarr,0) else nvl(zarr,0) end as arr
  , arr * 1.0 / 12 as mrr
from zuora
full outer join sfdc using (accountid, idtype, yearmonth, yearmonthvalue, fiscalyearmonth, fiscalperiod, fiscalyear)
