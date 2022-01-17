with referrals_summary as (
    select
        referrerID as userid,
        count(distinct referralid) as count_people_referred,
        count(distinct case when referralStatus = 'signed_up' then referralid end) as count_referrals_accepted
    from {{ ref('stg_vidyard_referrals') }}
    -- from dbt_vidyard_master.stg_vidyard_referrals
    group by 1
    ),

    admin_status as (
    select userid,
           sum(case when isadmin = True then 1 else 0 end) as admin_flag
    from {{ ref('tier2_vidyard_team_memberships') }}
    -- from dbt_vidyard_master.tier2_vidyard_team_memberships
    group by 1
    ),

    shared_video_summary as (
    select sharer_id,
           count(distinct pageview_id) as shared_count
    -- from dbt_vidyard_master.kube_vidyard_videos_viewers_sharers
    from {{ ref('kube_vidyard_videos_viewers_sharers')}}
    group by 1
    )


select u.userid,
       u.createddate,
       u.lastsession,
       u.personal_account_type,
       u.classification,
       u.enterprise_access,
       u.combined_usecase,
       rs.count_people_referred,
       rs.count_referrals_accepted,
       case when admin_status.admin_flag = 0 then False else True end as admin_flag,
       svs.shared_count
from {{ ref('kube_vidyard_user_details') }} u
-- from dbt_vidyard_master.kube_vidyard_user_details u
left join referrals_summary rs on rs.userid = u.userid
left join admin_status on admin_status.userid = u.userid
left join shared_video_summary svs on svs.sharer_id = u.userid


