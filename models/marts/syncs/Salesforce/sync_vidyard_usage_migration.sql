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

     account_wide_metrics_summary as (
         select o.accountid,
                sum(om.videoswithviews) as videos_with_views,
                sum(om.viewscount) as viewscount
         from dbt_vidyard_master.stg_vidyard_organizations o
         -- from {{ ref('stg_vidyard_organizations') }}
         left join dbt_vidyard_master.kube_org_wide_metrics om on om.organizationid = o.organizationid
         -- left join {{ ref('kube_org_wide_metrics') }} om on om.organizationid = o.organizationid
         group by 1
     ),

     video_summary as (
         select o.accountid,
                count(distinct case when origin != 'sample' then childentityid end) as videos,
                max(v.createddate) as last_video_date
         from dbt_vidyard_master.stg_vidyard_organizations o
         -- from {{ ref('stg_vidyard_organizations') }}
         left join dbt_vidyard_master.tier2_vidyard_videos v on v.organizationid = o.organizationid
         -- left join {{ ref('tier2_vidyard_videos') }} v on v.organizationid = o.organizationid
         group by 1
     ),

     embed_summary as (
         select accountid,
                sum(activeembeds) as active_embeds,
                sum(remaininembeds) as remaining_embeds,
                sum(allotmentlimit) as embed_limit
         from dbt_vidyard_master.tier2_embeds
         -- from {{ ref('tier2_embeds') }}
         group by 1
     ),

     account_feature_summary as(
         select accountid,
                sum(case when ssoenabled = True then 1 else 0 end) as sso,
                sum(case when gdprenabled = True then 1 else 0 end) as gdp,
                sum(case when seoenabled = True then 1 else 0 end) as seo
        from dbt_vidyard_master.tier2_vidyard_account_features
        -- from {{ ref('tier2_vidyard_account_features') }}
        group by 1
     ),

     admin_summary as (
             select accountid,
                    sum(case when isadmin = True then 1 else 0 end) as admin_count
             from dbt_vidyard_master.tier2_vidyard_team_memberships
             group by 1
     )

select distinct o.accountid,
                asum.seats_allotted,
                asum.seats_used,
                hs.hub_count,
                hs.bsp_count,
                vs.videos as video_count,
                vs.last_video_date as last_video_created_date,
                o.locked,
                o.lockeddate,
                vc.cta as cta_created,
                vc.cta_applied as videos_with_cta,
                vof.nve_activated,
--                 vof.nveactivateddate as nve_activated_date,
                awms.videos_with_views,
                awms.viewscount,
                es.active_embeds,
                es.remaining_embeds,
                es.embed_limit,
                admin.admin_count,
                case when afs.seo = 0 then False else True end as seo_enabled,
                case when afs.gdp = 0 then False else True end as gdp_enabled,
                case when afs.sso = 0 then False else True end as sso_enabled


from dbt_vidyard_master.stg_vidyard_organizations o
-- from {{ ref('stg_vidyard_organizations') }} o
left join allotment_summary asum on asum.accountid = o.accountid
left join hub_summary hs on hs.accountid = o.accountid
left join account_wide_metrics_summary awms on awms.accountid = o.accountid
left join dbt_vidyard_master.tier2_vidyard_ctas vc on vc.accountid = o.accountid
-- left join {{ ref('tier2_vidyard_ctas') }} vc on vc.accountid = o.accountid
left join video_summary vs on vs.accountid = o.accountid
left join dbt_vidyard_master.tier2_vidyard_organization_features vof on vof.accountid = o.accountid
-- left join {{ ref('tier2_vidyard_organization_features') }} vof on vof.accountid = o.accountid
left join embed_summary es on es.accountid = o.accountid
left join account_feature_summary afs on afs.accountid = o.accountid
left join admin_summary admin on admin.accountid = o.accountid