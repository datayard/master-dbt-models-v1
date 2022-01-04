with referrals_summary as (
    select
        referrerID as userid,
        count(distinct referralid) as count_people_referred,
        count(distinct case when referralStatus = 'signed_up' then referralid end) as count_referrals_accepted
    from {{ ref('stg_vidyard_referrals') }}
    group by 1
    ),

    admin_status as (
    select userid,
           sum(case when isadmin = True then 1 else 0 end) as admin_flag
    from {{ ref('tier2_vidyard_team_memberships') }}
    group by 1
    )

select u.userid,
       u.createddate,
       u.lastsession,
       rs.count_people_referred,
       rs.count_referrals_accepted,
       case when admin_status.admin_flag = 0 then False else True end as admin_flag
from {{ ref('kube_vidyard_user_details') }} u
left join referrals_summary rs on rs.userid = u.userid
left join admin_status on admin_status.userid = u.userid



