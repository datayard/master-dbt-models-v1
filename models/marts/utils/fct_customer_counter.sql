select case
        when z.sku = 'SS-010' then 'Pro Self Serve'
        when z.sku = 'SKU-00000009' or z.sku = 'SKU-00000020' then 'Enterprise Self Serve'
       end as accounttype,
       count(distinct z.accountid)
-- from dbt_vidyard_master.kube_zuora_ss_subscriptions z
from {{ ref('kube_zuora_ss_subscriptions') }} z
-- left join dbt_vidyard_master.tier2_vidyard_user_details u on u.organizationid = z.vidyardaccountid
left join {{ ref('tier2_vidyard_user_details') }} u on u.organizationid = z.vidyardaccountid
where z.promocodebillthrough
  and z.trailbillthrough
  and z.trailbillthroughnextday
  and z.sublongerthenoneday
  and u.classification != 'enterprise user'
group by 1


