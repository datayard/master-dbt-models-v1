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
--         inner join dbt_vidyard_master.stg_vidyard_organizations o on o.organizationid = v.organizationid
        inner join {{ ref('stg_vidyard_organizations') }} o on o.organizationid = v.organizationid
        where o.orgtype = 'self_serve'
        group by 1
    ),
    ranked_players as (
        select organizationid,
               uuid,
               row_number() over (partition by organizationid order by createddate desc) as rn
--         from dbt_vidyard_master.stg_vidyard_players
        from {{ ref('stg_vidyard_players') }}
    ),

    highlight_uuid as (
        select organizationid,
               uuid
        from ranked_players
        where rn = 1
    ),

    chrome_summary as (
        select vidyarduserid,
               max(chrome.sessiontime) as last_chrome_extension_session
        --from dbt_vidyard_master.stg_govideo_production_opened_extension chrome
        from {{ ref('stg_govideo_production_opened_extension') }} chrome
--         left join dbt_vidyard_master.stg_govideo_production_users u
        left join {{ ref('stg_govideo_production_users') }} u
        on chrome.userid = u.userid
        and u.identifier is not null
        group by 1
    ),
        zuora_summary as (
        select z.vidyardaccountid,
                MAX(case when z.subscription_type = 'Active - Pro' AND latest_subscription = true then subscriptionstartdate end) as Pro_upgrade
            from dbt_vidyard_master.tier2_zuora z
            --from {{ ref('tier2_zuora') }} z
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
       z.Pro_upgrade,
       e.allotmentlimit,
       e.remaininembeds,
       case when orgtype = 'self_serve' then o.createddate end as signup_date,
       o.createdbyclientid as signup_url,
       case when t2_meu.userid is not null then True else False end as meu,
       hu.uuid as highlight_video,
       cs.last_chrome_extension_session,
       heapu.generalUseCase,
       heapu.specificUseCase,
       heapu.confidence_survey
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
left join zuora_summary z on z.vidyardaccountid = o.organizationid
-- left join dbt_vidyard_master.tier2_embeds e on e.accountid = o.organizationid
left join {{ ref('tier2_embeds') }} e on e.accountid = o.organizationid
left join {{ ref('tier2_meu') }} t2_meu on t2_meu.organizationid = o.organizationid
-- left join dbt_vidyard_master.tier2_meu t2_meu on t2_meu.vidyardaccountid = o.organizationid
left join highlight_uuid hu on hu.organizationid = o.organizationid
left join chrome_summary cs on cs.vidyarduserid = o.ownerid
left join {{ ref('tier2_heap_users') }} heapu on heapu.vidyarduserid = o.ownerid
where o.orgtype = 'self_serve'
