with zuora as (
select
      case when a.crmid is null then a.accountnumber else left(a.crmid, 15) end as accountid15
      , case when a.crmid is null then 'zid' else 'accountid' end as idtype
      , a.accountname
      , rpc.effectivestartdate as contractstartdate

      , to_char(date_trunc('month', rpc.effectivestartdate), 'yyyy-mm') as contractstartmonth
                , rpc.effectiveenddate as contractenddate
     , case when date_part('day',rpc.effectiveenddate) >= 28 and date_part('day',rpc.effectivestartdate) = 1
          then to_char(date_trunc('month', rpc.effectiveenddate+4), 'yyyy-mm')
          else to_char(date_trunc('month', rpc.effectiveenddate), 'yyyy-mm') end as contractendmonth
          ,  datediff('month',rpc.effectivestartdate,rpc.effectiveenddate) as contractlength
          , case when contractlength <= 1 then 'monthly' when contractlength <= 10 then 'subannually' when contractlength <= 18 then 'annually' else 'multiyear' end as contractlengthtype

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
      , 2,3,4,5,6,7,8,9

)
, sfdc as (
  select accountid from {{ref('tier2_salesforce_account')}}
)

    select
      nvl(a.accountid , z.accountid15) as accountid
      , z.accountname
      , z.idtype
      , z.contractstartdate
      , z.contractstartmonth
      , z.contractenddate
      , z.contractendmonth
      , z.contractlength
      , z.contractlengthtype
      , z.arr
    from zuora z
    left join sfdc a on left(a.accountid,15) = z.accountid15
  where
    arr > 0
