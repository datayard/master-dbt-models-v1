
with
    use_case as (
        select o.organizationid
             , u.email
             , u.userid
             , hu.user_id as heap_userid
             , date(o.createddate) as createddate
             , datediff('month', o.createddate,current_date) as user_age_months
             , case when om.firstview is not null then '1' else 0 end as activated_flag
             , date(om.firstviewdate) as activated_date
             , hu.usecase
             , case
                 when hu.usecase ilike '%skip%' then null
                 when hu.usecase ilike '%customer%' then 'customer-success'
                 when hu.usecase ilike '%other%' and hu.general_use_case is not null and hu.general_use_case not ilike'%skip%' then null
                 else hu.usecase
               end as usecase_c
             , hu.general_use_case
             , case
                 when hu.general_use_case ilike '%skip%' then null
                 when hu.general_use_case ilike '%customer%' then 'customer-success'
                 when hu.general_use_case ilike '%other%' and hu.usecase is not null and hu.usecase not ilike'%skip%' then null
                 else hu.general_use_case
               end as general_use_case_c
            , lower(coalesce(general_use_case_c,usecase_c)) as combined_usecase
            , u.domain_type
        from dbt_vidyard_master.stg_vidyard_organizations o
                 join dbt_vidyard_master.stg_vidyard_users u
                      on u.userid = o.ownerid
                 left join dbt_vidyard_master.stg_vidyard_org_metrics om
                      on o.organizationid = om.organizationid
                 left join govideo_production.users hu
                           on u.userid = cast(hu."identity" as varchar(10))
        where u.email not like '%vidyard.com'
          and u.email not like '%viewedit.com'
          and o.orgtype = 'self_serve'
          and o.createdbyclientid NOT IN ('app.hubspot.com', 'marketing.hubspot.com', 'marketing.hubspotqa.com', 'Enterprise')
    ),
     missing_usecase as (
        select distinct
            organizationid
            , userid
            , heap_userid
            , email
            , createddate
            , user_age_months
            , activated_flag
            , activated_date
        from (
                 select uc.*
                      , row_number()
                        over (partition by uc.organizationid order by case when uc.combined_usecase is null then 99 else 1 end asc) as rn
                 from use_case uc
             ) a
        where
            rn=1
            and combined_usecase is null
            and domain_type = 'business'
            and activated_flag = 1
         ),
     product_usage as (
         select
              mu.organizationid
              , count(distinct oe.session_id) as frequency
              , datediff('day', max(oe.session_time), current_date) as recency
              , 'opened_extension' as event
         from missing_usecase mu
         join govideo_production.opened_extension oe
            on mu.heap_userid = oe.user_id
         where
            oe.time >= (DATEADD(year, -1, GETDATE()))
         group by 1
         UNION
          select
              mu.organizationid
              , count(distinct pa.session_id) as frequency
              , datediff('day', max(pa.session_time), current_date) as recency
              , 'partner_app' as event
         from missing_usecase mu
         join govideo_production.partner_app_viewed pa
            on mu.heap_userid = pa.user_id
         where
            pa.time >= (DATEADD(year, -1, GETDATE()))
         group by 1
         UNION
          select
              mu.organizationid
              , count(distinct l.session_id) as frequency
              , datediff('day', max(l.session_time), current_date) as recency
              , 'library' as event
         from missing_usecase mu
         join govideo_production.library_opened_library l
            on mu.heap_userid = l.user_id
         where l.time >= (DATEADD(year, -1, GETDATE()))
         group by 1
     ),
     pivoted as (
         select
            organizationid
            , sum(case when event = 'opened_extension' and frequency is not null then frequency else CAST(NULL AS bigint) end) as oe_frequency
            , sum(case when event = 'opened_extension' and recency is not null then recency else CAST(NULL AS bigint) end) as oe_recency
            , sum(case when event = 'partner_app' and frequency is not null then frequency else CAST(NULL AS bigint) end) as pa_frequency
            , sum(case when event = 'partner_app' and recency is not null then recency else CAST(NULL AS bigint) end) as pa_recency
            , sum(case when event = 'library' and frequency is not null then frequency else CAST(NULL AS bigint) end) as lb_frequency
            , sum(case when event = 'library' and recency is not null then recency else CAST(NULL AS bigint) end) as lb_recency
         from product_usage
         group by 1
     )
         select distinct
            mu.organizationid
            , mu.createddate
            , mu.user_age_months
            , mu.activated_date
            , pv.oe_frequency
            , pv.oe_recency
            , pv.pa_frequency
            , pv.pa_recency
            , pv.lb_frequency
            , pv.lb_recency
         from missing_usecase mu
         left join pivoted pv
            on mu.organizationid = pv.organizationid