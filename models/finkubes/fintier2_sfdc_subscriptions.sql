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
, salesforce as (
  select
    accountid
    , opportunityid
    , c.region
    , to_char(date_trunc('month', contractstartdate), 'YYYY-MM') as startyearmonth
    , to_char(date_trunc('month', contractenddate), 'YYYY-MM') as endyearmonth
    , lower(stagename) as stagename
    , opportunitytype
    , nvl(newarr, 0) as newarr
    , nvl(lastyeararr, 0) as lastyeararr
    , nvl(renewalamount, 0) as renewalarr
    , nvl(renewalwonarr, 0) as renewalwonarr
    , nvl(renewallostarr, 0) as renewallostarr
  from
    {{ref('tier2_salesforce_opportunity')}}
    left join
      {{ref('tier2_salesforce_account')}} a using (accountid)
    left join {{ref('fct_sfdc_country_to_region')}} c on lower(a.billingcountry) = c.country
  where
    (lower(stagename) like '%dead%' or lower(stagename) like '%close%')
    and contractstartdate <= contractenddate
)
, salesforce_timeline as (
  select
    s.accountid
    , s.opportunityid
    , s.region
    , s.startyearmonth
    , s.endyearmonth
    , d.yearmonthvalue
    , d.yearmonth
    , d.fiscalyearmonth
    , d.fiscalquarter
    , d.fiscalyear
    , case
      when stagename like '%dead%'
      and renewallostarr <> 0
        then renewallostarr
      when stagename like '%dead%'
        then - lastyeararr
      else 0
    end as lostarr
    , case
      when stagename like '%close%'
      and renewalwonarr > 0
        then renewalwonarr
      else 0
    end as renewalarr
    , case
      when stagename like '%close%'
        then newarr
      else 0
    end as wonarr
  from
    salesforce s
    left join dates_table d on
      yearmonth >= startyearmonth
      and yearmonth <= endyearmonth
  where
    wonarr + renewalarr - lostarr > 0
    and yearmonth is not null
)
, salesforce_max_renewal as (
  select
    accountid
    , yearmonthvalue
    , max(startyearmonth) as startyearmonth
  from
    salesforce_timeline
  where
    renewalarr > 0
  group by
    1
    , 2
)
, timeline_w_max_renewal as (
  select
    o.accountid
    , o.opportunityid
    , o.region
    , o.yearmonthvalue
    , o.yearmonth
    , o.fiscalyearmonth
    , o.fiscalquarter
    , o.fiscalyear
    , nvl(m.startyearmonth, '1900-01') as startyearmonth
    , sum(o.wonarr) as wonarr
    , sum(o.renewalarr) as renewalarr
    , sum(o.lostarr) as lostarr
  from
    salesforce_timeline o
    left join salesforce_max_renewal m on
      m.accountid = o.accountid
      and m.yearmonthvalue = o.yearmonthvalue
      and m.startyearmonth <= o.startyearmonth
  group by
    1
    , 2
    , 3
    , 4
    , 5
    , 6
    , 7
    , 8
)
, max_of_max as (
  select
    accountid
    , yearmonthvalue
    , max(startyearmonth) as startyearmonth
  from
    timeline_w_max_renewal
  group by
    1
    , 2
)
, salesforce_final as (
  select
    accountid
    , 'accountid' as idtype
    , region
    , yearmonth
    , yearmonthvalue
    , fiscalyearmonth
    , fiscalquarter
    , fiscalyear
    , sum(
      case
        when wonarr + renewalarr + lostarr < 0
          then 0
        else wonarr + renewalarr + lostarr
      end
      + case when opportunityid = '0064O00000kE8WQQA0' then 945000 else 0 end
    )
    as sarr
  from
    timeline_w_max_renewal
    right join max_of_max using (accountid, yearmonthvalue, startyearmonth)
  group by
    1
    , 2
    , 3,4,5,6,7
)

select *
from salesforce_final
