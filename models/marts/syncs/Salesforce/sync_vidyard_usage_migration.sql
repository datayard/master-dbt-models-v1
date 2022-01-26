with allotment_summary as (
         select va.accountid,
                sum(distinct case when va.allotmenttypeid = 1 then va.allotmentlimit end) as seats_allotted,
                count(distinct case when  vtm.inviteaccepted  = 'yes' then vtm.userid end ) as seats_used
--          from dbt_vidyard_master.tier2_vidyard_allotments va
         from {{ ref('tier2_vidyard_allotments') }} va
--          left join dbt_vidyard_master.tier2_vidyard_team_memberships vtm on vtm.accountid = va.accountid
         left join {{ ref('tier2_vidyard_team_memberships') }} vtm on vtm.accountid = va.accountid
         group by 1
     ),

     hub_summary as (
         select accountid,
                count(distinct case when hubtype = 'bsp' then hubid end) as bsp_count,
                count(distinct case when hubtype = 'hub' then hubid end) as hub_count
--          from dbt_vidyard_master.tier2_vidyard_hubs
         from {{ ref('tier2_vidyard_hubs') }}
         group by 1
     ),

     account_wide_metrics_summary as (
         select o.accountid,
                sum(om.videoswithviews) as videos_with_views,
                sum(om.viewscount) as viewscount
--          from dbt_vidyard_master.stg_vidyard_organizations o
         from {{ ref('stg_vidyard_organizations') }} o
--          left join dbt_vidyard_master.kube_org_wide_metrics om on om.organizationid = o.organizationid
         left join {{ ref('stg_vidyard_org_metrics') }} om on om.organizationid = o.organizationid
         group by 1
     ),

     video_summary as (
         select o.accountid,
                count(distinct case when origin != 'sample' and uc.classification in ('pro','free') then childentityid end) as free_pro_videos,
                count(distinct case when origin != 'sample' then childentityid end) as videos,
                max(v.createddate) as last_video_date
--          from dbt_vidyard_master.stg_vidyard_organizations o
         from {{ ref('stg_vidyard_organizations') }} o
--          left join dbt_vidyard_master.tier2_vidyard_videos v on v.organizationid = o.organizationid
         left join {{ ref('tier2_vidyard_videos') }} v on v.organizationid = o.organizationid
         left join {{ ref('tier2_users_classification') }} uc on uc.userid = v.userid
         group by 1
     ),

     embed_summary as (
         select
                distinct accountid,
                allotmentlimit as embed_limit,
                case when allotmentlimit = -1 then -1 else remaininembeds end as remaining_embeds,
                activeembeds as active_embeds
                --sum(activeembeds) as active_embeds
--          from dbt_vidyard_master.tier2_embeds
         from {{ ref('tier2_embeds') }}
         group by 1,2,3,4
     ),

     account_feature_summary as(
         select accountid,
                sum(case when ssoenabled = True then 1 else 0 end) as sso,
                sum(case when gdprenabled = True then 1 else 0 end) as gdp,
                sum(case when seoenabled = True then 1 else 0 end) as seo
--         from dbt_vidyard_master.tier2_vidyard_account_features
        from {{ ref('tier2_vidyard_account_features') }}
        group by 1
     ),

     admin_summary as (
         select accountid,
                sum(case when isadmin = True then 1 else 0 end) as admin_count
--          from dbt_vidyard_master.tier2_vidyard_team_memberships
         from {{ ref('tier2_vidyard_team_memberships') }}
         group by 1
     ),

     team_summary as (
         select accountid,
                count(distinct teamid) as teams_count
--          from dbt_vidyard_master.stg_vidyard_teams
         from {{ ref('stg_vidyard_teams') }}
         group by 1

     ),

     free_pro_mau_summary as (
         select m.accountid,
                count(distinct case when mau = 1 and uc.classification in ('enterprise user','enterprise self serve', 'hybrid') then m.userid end) as enterprise_mau_count,
                count(distinct case when yau = 1 and uc.classification in ('enterprise user','enterprise self serve', 'hybrid') then m.userid end) as enterprise_yau_count,
                count(distinct case when wau = 1 and uc.classification in ('enterprise user','enterprise self serve', 'hybrid') then m.userid end) as enterprise_wau_count,
                count(distinct case when mau = 1 and uc.classification in ('pro','free') then m.userid end) as fp_mau_count
--         from dbt_vidyard_master.tier2_mau m
        from {{ ref('tier2_mau') }} m
--         left join dbt_vidyard_master.tier2_users_classification uc on uc.userid = m.userid
        left join {{ ref('tier2_users_classification') }} uc on uc.userid = m.userid
        group by 1
     ),

     parent_nve_summary as (
         select accountid,
                nve_activated,
                nveactivateddate
         from dbt_vidyard_master.tier2_vidyard_organization_features
         -- from {{ ref('tier2_vidyard_organization_features') }}
         where organizationid = accountid
     ),

     hub_allotment_summary as (
         select distinct accountid,
                         allotmentlimit
         from dbt_vidyard_master.tier2_vidyard_allotments
         -- from {{ ref('tier2_vidyard_allotments') }}
         where allotmenttypeid = 5
     ),

     free_pro_meu_summary as (
         select m.accountid,
                count(distinct case when uc.classification in ('enterprise user','enterprise self serve', 'hybrid') then m.userid end) as enterprise_meu_count,
                count(distinct case when uc.classification in ('pro','free') then m.userid end) as fp_meu_count
--         from dbt_vidyard_master.tier2_mau m
        from {{ ref('tier2_meu') }} m
--         left join dbt_vidyard_master.tier2_users_classification uc on uc.userid = m.userid
        left join {{ ref('tier2_users_classification') }} uc on uc.userid = m.userid
        where uc.classification in ('enterprise user','enterprise self serve', 'hybrid')
        group by 1
     ),


     video_share_summary as (
       SELECT
          uc.accountid,
          COUNT(DISTINCT heap.eventid  ) AS shared_count,
          count(distinct case when uc.classification in ('free','pro') then heap.eventid end ) as free_pro_shared_count
      FROM
          dbt_vidyard_master.tier2_heap AS heap
       INNER JOIN 
              {{ ref('tier2_users_classification') }} uc 
       ON uc.userid = heap.vidyarduserid
      WHERE
          heap.tracker  = 'sharing_share_combo'
      GROUP BY
          1
     ),

     free_signups as (
         select uc.accountid,
                count(distinct case when datediff(day, createddate, getdate()) <=  30 then u.userid end) as last_30_days,
                count(distinct case when datediff(day, createddate, getdate()) <=  7 then u.userid end) as last_7_days
         -- from dbt_vidyard_master.kube_vidyard_user_details u
         from {{ ref('kube_vidyard_user_details') }} u
         -- left join dbt_vidyard_master.tier2_users_classification uc on uc.userid = u.userid
         left join {{ ref('tier2_users_classification') }} uc on uc.userid = u.userid
         where uc.classification = 'free'
         group by 1
     ),
     free_pro_embeds as (
         select e.accountid,
                allotmentlimit
         from {{ ref('tier2_embeds') }} e
         left join {{ ref('tier2_users_classification') }} uc on uc.organizationid = e.organizationid
         where uc.classification in ('free','pro')
     )


select distinct o.accountid,
                asum.seats_allotted,
                asum.seats_used,
                asum.seats_allotted - asum.seats_used as unused_seats,
                hs.hub_count,
                hs.bsp_count,
                case when hs.bsp_count = 0 then False else True end as bsp_setup,
                vs.videos as video_count,
                vs.free_pro_videos as free_pro_video_count,
                vss.shared_count,
                vss.free_pro_shared_count,
                vs.last_video_date as last_video_created_date,
                o.locked,
                o.lockeddate,
                vc.cta as cta_created,
                vc.cta_applied as videos_with_cta,
                vof.nve_activated,
                vof.nveactivateddate as nve_activated_date,
                awms.videos_with_views,
                awms.viewscount,
                es.active_embeds,
                es.remaining_embeds,
                es.embed_limit,
                admin.admin_count,
                ts.teams_count,
                mau.enterprise_mau_count,
                mau.enterprise_yau_count,
                mau.enterprise_wau_count,
                mau.fp_mau_count,
                meu.enterprise_meu_count,
                meu.fp_meu_count,
                case when afs.seo = 0 then False else True end as seo_enabled,
                case when afs.gdp = 0 then False else True end as gdp_enabled,
                case when afs.sso = 0 then False else True end as sso_enabled,
                sfuse.usecase,
                has.allotmentlimit as hub_allotments,
                fs.last_7_days as free_signups_last_7_days,
                fs.last_30_days as free_signups_last_30_days,
                vi.integration,
                fpe.allotmentlimit as free_pro_embed_limit



-- from dbt_vidyard_master.stg_vidyard_organizations o
from {{ ref('stg_vidyard_organizations') }} o
left join allotment_summary asum on asum.accountid = o.accountid
left join hub_summary hs on hs.accountid = o.accountid
left join account_wide_metrics_summary awms on awms.accountid = o.accountid
-- left join dbt_vidyard_master.tier2_vidyard_ctas vc on vc.accountid = o.accountid
left join {{ ref('tier2_vidyard_ctas') }} vc on vc.accountid = o.accountid
left join video_summary vs on vs.accountid = o.accountid
left join parent_nve_summary vof on vof.accountid = o.accountid
left join embed_summary es on es.accountid = o.accountid
left join account_feature_summary afs on afs.accountid = o.accountid
left join admin_summary admin on admin.accountid = o.accountid
left join team_summary ts on ts.accountid = o.accountid
left join free_pro_mau_summary mau on mau.accountid = o.accountid
-- left join dbt_vidyard_master.sync_use_case_from_opps sfuse on sfuse.vidyardaccountid = o.accountid
left join {{ ref('sync_use_case_from_opps') }} sfuse on sfuse.vidyardaccountid = o.accountid
left join hub_allotment_summary has on has.accountid = o.accountid
left join free_pro_meu_summary meu on meu.accountid = o.accountid
left join video_share_summary vss on vss.accountid = o.accountid
left join free_signups fs on fs.accountid = o.accountid
left join {{ ref('tier2_vidyard_integrations') }} vi on vi.accountid = o.accountid
left join free_pro_embeds fpe on fpe.accountid = o.accountid