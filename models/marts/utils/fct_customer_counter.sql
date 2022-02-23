select case
        when sku = 'SS-010' then 'Pro Self Serve'
        when sku = 'SKU-00000009' or sku = 'SKU-00000020' then 'Enterprise Self Serve'
       end as accounttype,
       count(distinct accountid)
-- from dbt_vidyard_master.kube_zuora_ss_subscriptions
from {{ ref('kube_zuora_ss_subscriptions') }}
where promocodebillthrough and trailbillthrough and trailbillthroughnextday and sublongerthenoneday
group by 1


