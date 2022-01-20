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
       wac.mac_count


from all_domains ad
left join mau_summary mau on mau.domain = ad.domain
left join meu_summary meu on meu.domain = ad.domain
left join weu_summary weu on weu.domain = ad.domain
left join wac_summary wac on wac.domain = ad.domain
-- left join dbt_vidyard_master.syncs_users_per_domain_match counts on counts.domain = ad.domain
left join {{ ref('syncs_users_per_domain_match') }} counts on counts.domain = ad.domain

