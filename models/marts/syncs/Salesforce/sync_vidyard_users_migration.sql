with referrals_summary as (
        select
            referrerID as userid,
            count(distinct referralid) as count_people_referred,
            count(distinct case when referralStatus = 'signed_up' then referralid end) as count_referrals_accepted
        from {{ ref('stg_vidyard_referrals') }}
--         from dbt_vidyard_master.stg_vidyard_referrals
        group by 1
    ),

    admin_status as (
        select userid,
               sum(case when isadmin = True then 1 else 0 end) as admin_flag
        from {{ ref('tier2_vidyard_team_memberships') }}
--         from dbt_vidyard_master.tier2_vidyard_team_memberships
        group by 1
    ),

    shared_video_summary as (
        select sharer_id,
               count(distinct pageview_id) as shared_count
--         from dbt_vidyard_master.kube_vidyard_videos_viewers_sharers
        from {{ ref('kube_vidyard_videos_viewers_sharers')}}
        group by 1
    ),

    vidyard_videos_summary as (
        select v.userid,
               count(distinct v.childentityid) as number_of_videos,
               max(o.createddate) as last_video_created_date
--         from dbt_vidyard_master.tier2_vidyard_videos v
        from {{ ref('tier2_vidyard_videos') }} v
--         inner join dbt_vidyard_master.stg_vidyard_organizations o on o.organizationid  = v.organizationid
        inner join {{ ref('stg_vidyard_organizations') }} o on o.organizationid  = v.organizationid
--         inner join dbt_vidyard_master.stg_vidyard_org_metrics om on om.organizationid = v.organizationid
        inner join {{ ref('stg_vidyard_org_metrics') }} om on om.organizationid = v.organizationid
        where o.orgtype = 'self_serve'
        group by 1
    )

select u.userid,
       u.organizationid,
       u.createddate,
       u.lastsession,
       u.personal_account_type,
       u.classification,
       u.enterprise_access,
       u.combined_usecase,
       rs.count_people_referred,
       rs.count_referrals_accepted,
       case when admin_status.admin_flag > 0 then True else False end as admin_flag,
       svs.shared_count,
       vvs.number_of_videos,
       vvs.last_video_created_date,
       u.videoswithviews,
       u.viewscount,
       u.activatedflag,
       ms.mau,
       case when z.subscription_type = 'Active - Pro' then True else false end as Pro_upgrade
from {{ ref('kube_vidyard_user_details') }} u
-- from dbt_vidyard_master.kube_vidyard_user_details u
left join referrals_summary rs on rs.userid = u.userid
left join admin_status on admin_status.userid = u.userid
left join shared_video_summary svs on svs.sharer_id = u.userid
left join vidyard_videos_summary vvs on vvs.userid = u.userid
-- left join dbt_vidyard_master.tier2_mau ms on ms.userid = u.userid
left join {{ ref('tier2_mau') }} ms on ms.userid = u.userid
-- left join dbt_vidyard_master.tier2_zuora z on z.vidyardaccountid = u.organizationid
left join {{ ref('tier2_zuora') }} z on z.vidyardaccountid = u.organizationid
