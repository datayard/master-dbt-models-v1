with gross_new_summary as (
  select accountid, yearmonth as grossnewmonth, yearmonthvalue as grossnewmonthvalue, changemrr as grossnewmrr, changearr as grossnewarr
  from {{ref('finkube_subscription_changes')}}
  where changetype = 'gross new'

)


select *
from {{ref('fintier2_subscriptions')}}
left join gross_new_summary using (accountid)
