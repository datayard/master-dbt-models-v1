with all_domains as (
    select distinct u.domain
--     from dbt_vidyard_master.kube_vidyard_user_details u
    from {{ ref('kube_vidyard_user_details') }} u
    ),

     mau_summary as (
        select m.domain,
               count(distinct case when m.mau = 1 then m.userid end) as mau_count,
               count(distinct case when m.yau = 1 then m.userid end) as yau_count,
               count(distinct case when m.wau = 1 then m.userid end) as wau_count

--         from dbt_vidyard_master.tier2_mau m
        from {{ ref('tier2_mau') }} m
        group by 1
    ),

    meu_summary as (
        select m.domain,
               count(distinct m.userid ) as meu_count
--         from dbt_vidyard_master.tier2_meu m
        from {{ ref('tier2_meu') }} m
        group by 1

    ),

    weu_summary as (
        select w.domain,
               count(distinct w.userid) as weu_count
--         from dbt_vidyard_master.tier2_weu w
        from {{ ref('tier2_weu') }} w
        group by 1
    ),
    wac_summary as (
        select v.domain,
               count(distinct case when v.createddate >= dateadd(day, -7, current_date) then v.userid end) as wac_count,
               count(distinct case when v.createddate >= dateadd(day, -30, current_date) then v.userid end) as mac_count
--         from dbt_vidyard_master.tier2_vidyard_videos v
        from {{ ref('tier2_vidyard_videos') }} v
        group by 1
    ),

    video_summary as (
         select u.domain,
                count(distinct case when origin != 'sample' and u.classification in ('pro','free') then childentityid end) as free_pro_videos,
                count(distinct case when origin != 'sample' and u.classification in ('pro') then childentityid end) as pro_videos,
                count(distinct case when origin != 'sample' then childentityid end) as videos,
                max(v.createddate) as last_video_date
         from  {{ ref('kube_vidyard_user_details') }} u
         left join {{ ref('tier2_vidyard_videos') }} v on u.userid = v.userid
         group by 1
    ),

    video_share_summary as (
          SELECT
              --uc.accountid,
                vud.domain,
--             sfdca.vidyardaccountid,
              COUNT(DISTINCT heap.eventid  ) AS shared_count,
              count(distinct case when uc.classification in ('free','pro') then heap.eventid end ) as free_pro_shared_count
          FROM
              dbt_vidyard_master.tier2_heap AS heap
           INNER JOIN
                  dbt_vidyard_master.tier2_users_classification uc --{{ ref('tier2_users_classification') }} uc
           ON uc.userid = heap.vidyarduserid
          JOIN
                  dbt_vidyard_master.tier2_vidyard_user_details vud
          ON uc.userid = vud.userid
--           JOIN dbt_vidyard_master.tier2_salesforce_account sfdca
--           ON vud.domain like sfdca.emaildomain
          WHERE
              heap.tracker  = 'sharing_share_combo'
            --AND uc.accountid = '229413'
            AND vud.domain like 'zoominfo.com'
--             AND sfdca.vidyardaccountid is not null
          GROUP BY
              1
     ),

      free_signups as (
         select u.domain,
                count(distinct case when datediff(day, createddate, getdate()) <=  30 then u.userid end) as last_30_days,
                count(distinct case when datediff(day, createddate, getdate()) <=  7 then u.userid end) as last_7_days
         -- from dbt_vidyard_master.kube_vidyard_user_details u
         from {{ ref('kube_vidyard_user_details') }} u
         -- left join dbt_vidyard_master.tier2_users_classification uc on uc.userid = u.userid
         left join {{ ref('tier2_users_classification') }} uc on uc.userid = u.userid
         where uc.classification = 'free'
         group by 1
     ),

     free_pro_cta as (
         select
            u.domain,
            count(distinct e.eventid) as free_pro_cta
--         from dbt_vidyard_master.stg_vidyard_events e
        from {{ ref('stg_vidyard_events') }} e
--         left join dbt_vidyard_master.stg_vidyard_organizations o on e.organizationid = o.organizationid
        left join {{ ref('stg_vidyard_organizations') }} o on e.organizationid = o.organizationid
--         inner join dbt_vidyard_master.kube_vidyard_user_details u on o.ownerid = u.userid
        inner join {{ ref('kube_vidyard_user_details') }} u on o.ownerid = u.userid
        where u.classification in ('free','pro')
        group by 1
     )

select ad.domain,
       mau.wau_count,
       mau.mau_count,
       mau.yau_count,
       meu.meu_count,
       weu.weu_count,
       counts.free as free_user_count,
       counts.pro as pro_user_count,
       counts.free + counts.pro as free_pro_total,
       wac.wac_count,
       wac.mac_count,
       vs.free_pro_videos,
       vs.pro_videos,
       vs.videos,
       vs.last_video_date,
       vss.shared_count,
       vss.free_pro_shared_count,
       fs.last_7_days,
       fs.last_30_days
from all_domains ad
left join mau_summary mau on mau.domain = ad.domain
left join meu_summary meu on meu.domain = ad.domain
left join weu_summary weu on weu.domain = ad.domain
left join wac_summary wac on wac.domain = ad.domain
-- left join dbt_vidyard_master.syncs_users_per_domain_match counts on counts.domain = ad.domain
left join {{ ref('syncs_users_per_domain_match') }} counts on counts.domain = ad.domain
left join video_summary vs on vs.domain = ad.domain
left join video_share_summary vss on vss.domain = ad.domain
left join free_signups fs on fs.domain = ad.domain
left join free_pro_cta fpc on fpc.domain = ad.domain