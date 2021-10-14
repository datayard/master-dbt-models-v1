select
    o.organizationid
    , o.accountid
    , om.totalseconds as totalSeconds
    , om.viewscount as viewsCount
    , om.videoswithviews as videosWithViews
    , count(distinct v.childentityid) as videos_count
    , count(distinct e.eventid) as events_count
    , count(distinct case when e.eventjoinid is not null then e.eventid else null end) as applied_events_count
    , count(distinct case when e.eventtype not like '%simple%' then e.eventjoinid else null end) as custom_vidoes
from {{ ref('stg_vidyard_organizations') }} o
left join {{ ref('tier2_vidyard_videos') }} v
    on o.organizationid = v.organizationid
left join {{ ref('tier2_vidyard_events') }} e
    on o.organizationid = e.organizationid
left join {{ ref('stg_vidyard_org_metrics') }} om
    on om.organizationid = o.organizationid
group by 1, 2, 3, 4, 5