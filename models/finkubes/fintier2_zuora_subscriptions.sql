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
 , zuora as (
select
      lower(case when a.crmid is null then a.accountnumber else left(a.crmid, 15) end) as accountid15
      , case when a.crmid is null then 'zid' else 'accountid' end as idtype
    --  , to_char(date_trunc('month', s.subscriptionstartdate), 'yyyy-mm') as startyearmonth
    --  , to_char(date_trunc('month', s.subscriptionenddate), 'yyyy-mm') as endyearmonth
      , to_char(date_trunc('month', rpc.effectivestartdate), 'yyyy-mm') as startyearmonth
     , case when date_part('day',rpc.effectiveenddate) >= 28 and date_part('day',rpc.effectivestartdate) = 1
          then to_char(date_trunc('month', rpc.effectiveenddate+4), 'yyyy-mm')
          else to_char(date_trunc('month', rpc.effectiveenddate), 'yyyy-mm') end as endyearmonth
      , sum(nvl(rpc.mrr * 12, 0)) as arr
      FROM {{ ref('stg_zuora_rate_plan') }} AS rp
               JOIN {{ ref('stg_zuora_subscription') }} AS s ON s.subscriptionid = rp.subscriptionid
               JOIN {{ ref('stg_zuora_rate_plan_charge')}} AS rpc ON rpc.rateplanid = rp.rateplanid
               JOIN {{ ref('stg_zuora_account') }} AS a ON a.accountid = s.accountid
               JOIN {{ ref('stg_zuora_product_rate_plan') }} AS prp ON prp.productrateplanid = rp.productrateplanid
               JOIN {{ ref('stg_zuora_product') }} AS p ON p.productid = prp.productid
               JOIN {{ ref('stg_zuora_contact') }} c ON c.contactid = s.soldtocontactid
    where
      s.fivetrandeleted = 'f'
      and s.status <> 'Expired'
      and p.name <> 'Open Balance'
      and nvl(rpc.mrr, 0) <> 0
    --  and rpc.end_date_condition <> 'OneTime'
    group by
      1
      , 2,3,4

)
, sfdc as (
  select lower(accountid) as accountid, region from {{ref('tier2_salesforce_account')}} a
  left join {{ref('fct_sfdc_country_to_region')}} c on lower(a.billingcountry) = c.country
)

--REPLACE ZUORA_PRO with ZUORA TABLES NOT ZUORA SUBSCRIPTION__C
, zuora_sfdc_id_adjust as (
    select
      nvl(a.accountid , z.accountid15) as accountid
      , a.region
      , z.idtype
      , z.startyearmonth
      , z.endyearmonth
      , z.arr
    from zuora z
    left join sfdc a on left(a.accountid,15) = z.accountid15
  )
, zuora_final as (
  select
    z.accountid
    , z.region
    , z.idtype
    , d.yearmonth
    , d.yearmonthvalue
    , d.fiscalyearmonth
    , d.fiscalquarter
    , d.fiscalyear
    , sum(arr) as zarr
  from
    zuora_sfdc_id_adjust z
    left join dates_table d on
      d.yearmonth >= z.startyearmonth
      and (d.yearmonth < z.endyearmonth or z.endyearmonth is null)
  where
    arr > 0
    and d.yearmonth is not null
  group by
    1
    , 2
    , 3,4,5,6,7,8
)

select * from zuora_final
