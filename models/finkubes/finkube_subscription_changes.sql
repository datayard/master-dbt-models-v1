with subscriptions as (
  select
    *
    , 1 as connector
  FROM
    {{ref('fintier2_subscriptions')}}
)

, dates_table as (
  select
    yearmonthvalue
    , yearmonth
    , fiscalyearmonth
    , fiscalquarter
    , fiscalyear
    , 1 as connector
 from {{ ref('fct_sfdc_dates') }}
 group by 1,2,3,4,5
)

, expand_dates as (
  select
    s.accountid
    , s.customertype
    , d.yearmonth
    , d.yearmonthvalue
    , d.fiscalyearmonth
    , d.fiscalquarter
    , d.fiscalyear
    , sum( case when d.yearmonth = s.yearmonth then nvl(mrr,0) else 0 end) as mrr
  from subscriptions s
  left join dates_table d using (connector)
  group by 1,2,3,4,5,6,7
)

, cumulative_expand_dates as (
  select
    *
    , sum(mrr) over(partition by accountid order by yearmonth rows between unbounded preceding and current row) as cumulativemrr
  from expand_dates
)

, self_join_for_prior_month as (
  select
    t0.accountid
    , t0.customertype
    , t0.yearmonth
    , t0.yearmonthvalue
    , t0.fiscalyearmonth
    , t0.fiscalquarter
    , t0.fiscalyear
    , t0.cumulativemrr
    , t0.mrr
    , nvl(t1.mrr,0) as previousmrr
  from cumulative_expand_dates as t0
  left join expand_dates as t1 on
    t0.yearmonthvalue - 1 = t1.yearmonthvalue and t0.accountid = t1.accountid
)

, summary as (
select
  accountid
  , customertype
  , yearmonth
  , yearmonthvalue
  , fiscalyearmonth
  , fiscalquarter
  , fiscalyear
  , case when mrr > previousmrr and cumulativemrr = mrr then 'gross new'
    when mrr > previousmrr and cumulativemrr > mrr and previousmrr = 0 then 'winback'
    when mrr > previousmrr and cumulativemrr > mrr and previousmrr > 0 then 'upsell'
    when mrr < previousmrr and mrr = 0 then 'churn'
    when mrr < previousmrr and mrr > 0 then 'downsell'
    end as changetype
  , mrr - previousmrr as changemrr
  , previousmrr
  , mrr as netmrr
  , changemrr * 12  as changearr
  , previousmrr * 12 as previousarr
  , mrr*12 as netarr
from self_join_for_prior_month
where mrr <> previousmrr
)

, gross_new_summary as (
select accountid, yearmonth as grossnewmonth, yearmonthvalue as grossnewmonthvalue, changemrr as grossnewmrr, changearr as grossnewarr
from summary
where changetype = 'gross new'

)

select *
from summary
left join gross_new_summary using (accountid)
