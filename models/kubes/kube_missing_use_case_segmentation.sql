
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
     extension_usage as (
         select
              mu.organizationid
              , count(distinct oe.session_id) as oe_frequency
              , datediff('day', max(oe.session_time), current_date) as oe_recency
         from missing_usecase mu
         join govideo_production.opened_extension oe
            on mu.heap_userid = oe.user_id
         where
            oe.time >= '2021-01-01'
         group by 1
     ),
     partner_app_usage as (
         select
              mu.organizationid
              , count(distinct pa.session_id) as pa_frequency
              , datediff('day', max(pa.session_time), current_date) as pa_recency
         from missing_usecase mu
         join govideo_production.partner_app_viewed pa
            on mu.heap_userid = pa.user_id
         where
            pa.time >= '2021-01-01'
         group by 1
     ),
     dashboard_usage as (
          select
              mu.organizationid
              , count(distinct l.session_id) as lb_frequency
              , datediff('day', max(l.session_time), current_date) as lb_recency
         from missing_usecase mu
         join govideo_production.library_opened_library l
            on mu.heap_userid = l.user_id
         where l.time >= '2021-01-01'
         group by 1
     )
select distinct
    mu.organizationid
    , mu.email
    , mu.createddate
    , mu.user_age_months
    , mu.activated_date
    , oe.oe_frequency
    , oe.oe_recency
    , pa.pa_frequency
    , pa.pa_recency
    , db.lb_frequency
    , db.lb_recency
from
missing_usecase mu
left join extension_usage oe
    on mu.organizationid = oe.organizationid
left join partner_app_usage pa
    on mu.organizationid = pa.organizationid
left join dashboard_usage db
    on mu.organizationid = pa.organizationid



