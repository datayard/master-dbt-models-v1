with zuora_enterprise_accounts as (
    select 
        distinct zuora.vidyardAccountId
    from {{ ref('tier2_zuora') }} zuora
    where 
        zuora.sku not in ('SS-010', 'SKU-00000009', 'SKU-00000020') 
        and zuora.subscription_type  like '%Active%'
)
select
    o.organizationid
    , o.accountid
    , o.name
    , a.accountname
    , o.orgtype    
    , om.totalseconds as totalSeconds
    , om.viewscount as viewsCount
    , om.videoswithviews as videosWithViews
    , count(distinct case when v.origin != 'sample' then v.childentityid else null end) as videos_count
    , count(distinct e.eventid) as events_count
    , count(distinct case when e.eventjoinid is not null then e.eventid else null end) as applied_events_count
    , count(distinct case when e.eventtype not like '%simple%' then e.eventjoinid else null end) as custom_vidoes
from {{ ref('stg_vidyard_organizations') }} o
join zuora_enterprise_accounts z
    on o.accountid = z.vidyardaccountid
join {{ ref('tier2_salesforce_account')}} a
    on a.vidyardaccountid = o.accountid
     and a.ispersonaccount = False    
left join {{ ref('tier2_vidyard_videos') }} v
    on o.organizationid = v.organizationid
left join {{ ref('tier2_vidyard_events') }} e
    on o.organizationid = e.organizationid
left join {{ ref('stg_vidyard_org_metrics') }} om
    on om.organizationid = o.organizationid
where o.orgtype != 'self_serve'    
group by 1, 2, 3, 4, 5, 6, 7, 8