with referrals_summary as (
        select
            o.organizationid,
            count(distinct r.referralid) as count_people_referred,
            count(distinct case when r.referralStatus = 'signed_up' then r.referralid end) as count_referrals_accepted
        from {{ ref('stg_vidyard_referrals') }} r
--         from dbt_vidyard_master.stg_vidyard_referrals r
        inner join {{ ref('stg_vidyard_organizations') }} o on o.ownerid = r.referrerID
        where o.orgtype = 'self_serve'
        group by 1
    ),

    admin_status as (
        select o.organizationid,
               sum(case when tm.isadmin = True then 1 else 0 end) as admin_flag
        from {{ ref('tier2_vidyard_team_memberships') }} tm
--         from dbt_vidyard_master.tier2_vidyard_team_memberships
        inner join {{ ref('stg_vidyard_organizations') }} o on o.ownerid = tm.userid
        -- inner join dbt_vidyard_master.stg_vidyard_organizations o on o.ownerid = tm.userid
        where o.orgtype = 'self_serve'
        group by 1
    ),

    shared_video_summary as (
        select o.organizationid,
               count(distinct pageview_id) as shared_count
--         from dbt_vidyard_master.kube_vidyard_videos_viewers_sharers vs
        from {{ ref('kube_vidyard_videos_viewers_sharers') }} vs
        inner join {{ ref('stg_vidyard_organizations') }} o on o.ownerid = vs.sharer_id
        -- inner join dbt_vidyard_master.stg_vidyard_organizations o on o.ownerid = vs.sharer_id
        group by 1
    ),

    vidyard_videos_summary as (
        select o.organizationid,
               count(distinct v.childentityid) as number_of_videos,
               max(v.createddate) as last_video_created_date
--         from dbt_vidyard_master.tier2_vidyard_videos v
        from {{ ref('tier2_vidyard_videos') }} v
--         inner join dbt_vidyard_master.stg_vidyard_organizations o on o.ownerid = v.userid
        inner join {{ ref('stg_vidyard_organizations') }} o on o.ownerid = v.userid
        where o.orgtype = 'self_serve'
        group by 1
    )

select distinct
       u.userid,
       o.organizationid,
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
       case when z.subscription_type = 'Active - Pro' then subscriptionstartdate end as Pro_upgrade,
       e.allotmentlimit,
       e.remaininembeds,
       case when orgtype = 'self_serve' then o.createddate end as signup_date,
       o.createdbyclientid as signup_url
from {{ ref('stg_vidyard_organizations') }} o
-- from dbt_vidyard_master.stg_vidyard_organizations o
inner join {{ ref('kube_vidyard_user_details') }} u on o.ownerid = u.userid
-- inner join dbt_vidyard_master.kube_vidyard_user_details u on o.ownerid = u.userid
left join referrals_summary rs on rs.organizationid = o.organizationid
left join admin_status on admin_status.organizationid = o.organizationid
left join shared_video_summary svs on svs.organizationid = o.organizationid
left join vidyard_videos_summary vvs on vvs.organizationid = o.organizationid
-- left join dbt_vidyard_master.tier2_mau ms on ms.organizationid = o.organizationid
left join {{ ref('tier2_mau') }} ms on ms.organizationid = o.organizationid
-- left join dbt_vidyard_master.tier2_zuora z on z.vidyardaccountid = o.organizationid
left join {{ ref('tier2_zuora') }} z on z.vidyardaccountid = o.organizationid
-- left join dbt_vidyard_master.tier2_embeds e on e.accountid = o.organizationid
left join {{ ref('tier2_embeds') }} e on e.accountid = o.organizationid
where o.orgtype = 'self_serve'

