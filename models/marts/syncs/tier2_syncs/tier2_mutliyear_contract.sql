with
  opps as (
    select
      a.accountid,
      a.accountname,
      a.accountType,
      o.opportunityname,
      o.contractstartdate,
      o.contractenddate,
      o.termlength,
      row_number() over(partition by o.accountid order by o.termlength desc)
    from
        --{{ ref('stg_salesforce_account') }} a
        dbt_vidyard_master.stg_salesforce_account a
    left join
        --{{ ref('stg_salesforce_opportunity') }} o
        dbt_vidyard_master.stg_salesforce_opportunity o
    on
        o.accountid = a.accountid
    where
      o.iswon is true
      and o.contractenddate >= current_date
  )

select
  accountid,
  accountname,
  accounttypee,
  case
    when termlength >= 729
      then 1
    else 0
  end as multi_year
from
  opps
where
  row_number = 1
order by
  4 desc
  , 2