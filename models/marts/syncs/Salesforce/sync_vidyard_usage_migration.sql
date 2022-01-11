with allotment_summary as (
         select va.accountid,
                sum(distinct case when va.allotmenttypeid = 1 then va.allotmentlimit end) as seats_allotted,
                count(distinct case when  vtm.inviteaccepted  = 'yes' then vtm.userid end ) as seats_used
         from dbt_vidyard_master.tier2_vidyard_allotments va
         -- from {{ ref('tier2_vidyard_allotments') }}
         left join dbt_vidyard_master.tier2_vidyard_team_memberships vtm on vtm.accountid = va.accountid
         -- left join {{ ref('tier2_vidyard_team_memberships') }}
         group by 1
     ),

     hub_summary as (
         select accountid,
                count(distinct case when hubtype = 'bsp' then hubid end) as bsp_count,
                count(distinct case when hubtype = 'hub' then hubid end) as hub_count
         from dbt_vidyard_master.tier2_vidyard_hubs
         -- from {{ ref('tier2_vidyard_hubs') }}
         group by 1
     ),

     account_video_summary as (
         select o.accountid,
                sum(om.videoswithviews) as videos_with_views,
                sum(om.viewscount) as viewscount,
                sum(om.videos_count) as videos
         from dbt_vidyard_master.stg_vidyard_organizations o
         -- from {{ ref('stg_vidyard_organizations') }}
         left join dbt_vidyard_master.kube_org_wide_metrics om on om.organizationid = o.organizationid
         -- left join {{ ref('kube_org_wide_metrics') }} om on om.organizationid = o.organizationid
         group by 1
     ),

     last_video_summary as (
         select o.accountid,
                max(v.createddate) as last_video_date
         from dbt_vidyard_master.stg_vidyard_organizations o
         -- from {{ ref('stg_vidyard_organizations') }}
         left join dbt_vidyard_master.tier2_vidyard_videos v on v.organizationid = o.organizationid
         -- left join {{ ref('tier2_vidyard_videos') }} v on v.organizationid = o.organizationid
         group by 1
     )



select distinct o.accountid,
                asum.seats_allotted,
                asum.seats_used,
                hs.hub_count,
                hs.bsp_count,
                avs.videos,
                lvs.last_video_date as last_video_created_date,
                o.locked,
                o.lockeddate,
                vc.cta as cta_created,
                vc.cta_applied as videos_with_cta,
                avs.videos_with_views,
                avs.viewscount


from dbt_vidyard_master.stg_vidyard_organizations o
-- from {{ ref('stg_vidyard_organizations') }}
left join allotment_summary asum on asum.accountid = o.accountid
left join hub_summary hs on hs.accountid = o.accountid
left join account_video_summary avs on avs.accountid = o.accountid
left join dbt_vidyard_master.tier2_vidyard_ctas vc on vc.accountid = o.accountid
-- left join {{ ref('tier2_vidyard_ctas') }} vc on vc.accountid = o.accountid
left join last_video_summary lvs on lvs.accountid = o.accountid