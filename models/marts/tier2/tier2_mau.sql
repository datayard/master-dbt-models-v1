with heap_product_sessions as (
    select
        identifier as vy_userid
        , min(sessiontime) as first_session
        , max(sessiontime) as last_session
        , COUNT(DISTINCT sessiontime) as num_sessions
    from {{ ref('tier2_heap') }} as heap
    --from dbt_vidyard_master.tier2_heap as heap
    where heap.tracker = 'product_sessions'
    group by identifier
)
select
    usrdet.userid

    , case
        when hps.last_session >= dateadd(day, -30, current_date) then 1
        else 0
      end as MAU
    , case
        when hps.last_session >= dateadd(day, -365, current_date) then 1
        else 0
      end as YAU
    , case
        when hps.last_session >= dateadd(day, -7, current_date) then 1
        else 0
      end as WAU
    , usrdet.organizationid
    , usrdet.accountid
    , usrdet.personal_account_type
    , usrdet.enterprise_access
    , usrdet.classification
    , usrdet.excludeEmail
    , usrdet.domain
    , usrdet.domainType
    , usrdet.viewscount
    
from
    {{ ref('tier2_vidyard_user_details') }} as usrdet
    --dbt_vidyard_master.tier2_vidyard_user_details as usrdet
    left join heap_product_sessions hps
        on hps.vy_userid = usrdet.userid and usrdet.classification NOT IN ('anomaly','orphan','unknown')


