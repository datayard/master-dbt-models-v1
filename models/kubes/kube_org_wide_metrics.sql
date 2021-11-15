with zuora_enterprise_accounts as (
    select
        distinct zuora.vidyardAccountId
    -- from dbt_vidyard_master.tier2_zuora zuora
    from {{ ref('tier2_zuora') }} zuora
    where
        zuora.sku not in ('SS-010', 'SKU-00000009', 'SKU-00000020')
        and zuora.subscription_type  like '%Active%'
),
     kube_org_wide_metrics as (
         select o.organizationid
              , o.accountid
              , o.parentid
              , o.name as name
              , a.accountname
              , o.orgtype
              , om.totalseconds                                                                             as totalSeconds
              , om.viewscount                                                                               as viewsCount
              , om.videoswithviews                                                                          as videosWithViews
              , count(distinct case when v.origin != 'sample' then v.childentityid else null end)           as videos_count
              , count(distinct e.eventid)                                                                   as events_count
              , count(distinct case when e.eventjoinid is not null then e.eventid else null end)            as applied_events_count
              , count(distinct case when e.eventtype not like '%simple%' then e.eventjoinid else null end)  as custom_videos
        --  from dbt_vidyard_master.stg_vidyard_organizations o
        from {{ ref('stg_vidyard_organizations') }} o
                join zuora_enterprise_accounts z
                    on o.accountid = z.vidyardaccountid
                join {{ ref('tier2_salesforce_account') }} a                
                -- join dbt_vidyard_master.tier2_salesforce_account a
                    on a.vidyardaccountid = o.accountid
                    and a.ispersonaccount = False
                join {{ ref('tier2_vidyard_videos') }} v 
                -- left join dbt_vidyard_master.tier2_vidyard_videos v
                    on o.organizationid = v.organizationid
                join {{ ref('tier2_vidyard_events') }} e                     
                -- left join dbt_vidyard_master.tier2_vidyard_events e
                    on o.organizationid = e.organizationid
                join {{ ref('stg_vidyard_org_metrics') }} om                         
                -- left join dbt_vidyard_master.stg_vidyard_org_metrics om
                    on om.organizationid = o.organizationid
         group by 1, 2, 3, 4, 5, 6, 7, 8, 9
     ),
    self_serve_summary as (
        select
            o.accountid
            , o.parentid
            , o.accountname
            , o.orgtype
            , sum(o.totalSeconds)           as totalSeconds
            , sum(o.viewsCount)             as viewsCount
            , sum(o.videosWithViews)        as videosWithViews
            , sum(o.videos_count)           as videos_count
            , sum(o.events_count)           as events_count
            , sum(o.applied_events_count)   as applied_events_count
            , sum(o.custom_videos)          as custom_videos
        from kube_org_wide_metrics o
        where
            o.orgtype = 'self_serve'
        group by 1,2,3,4
    )
select
    k.organizationid
    , k.accountid
    , k.parentid
    , k.name as name
    , k.accountname
    , k.orgtype
    , k.totalSeconds + isnull(ss.totalSeconds,0)                    as totalSeconds
    , k.viewsCount + isnull(ss.viewsCount,0)                        as viewsCount
    , k.videosWithViews + isnull(ss.videosWithViews,0)              as videosWithViews
    , k.videos_count + isnull(ss.videos_count,0)                    as videos_count
    , k.events_count + isnull(ss.events_count,0)                    as events_count
    , k.applied_events_count + isnull(ss.applied_events_count,0)    as applied_events_count
    , k.custom_videos + isnull(ss.custom_videos,0)                  as custom_videos
from kube_org_wide_metrics k
left join self_serve_summary ss
    on k.organizationid = ss.parentid
where (k.orgtype != 'self_serve' or k.orgtype is null)