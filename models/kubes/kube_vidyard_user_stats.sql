with videos as 
(
    select
        u.userid,
        max(case when  v.origin  != 'sample' then v.createddate end) as last_player_created
    from {{ ref('kube_vidyard_user_details') }} u        
    -- from dbt_vidyard_master.kube_vidyard_user_details u
    join {{ ref('tier2_vidyard_videos') }} v
    -- join dbt_vidyard_master.tier2_vidyard_videos  v 
        on u.userid = v.userid
    group by 1
),
heap as 
(
    select
        u.userid,
        max(case when h.tracker = 'product_sessions' then h.sessiontime ELSE NULL end) AS last_product_session,
        max(case when h.tracker IN ('video_creation_create_combo', 'sharing_share_combo','insights_analytics_combo', 'manage_combo', 'admin_combo') then h.eventtime ELSE NULL end) AS last_event_time
    from {{ ref('kube_vidyard_user_details') }} u 
    -- from dbt_vidyard_master.kube_vidyard_user_details u
    join {{ ref('tier2_heap') }} h
    -- join dbt_vidyard_master.tier2_heap h 
        on u.userid = h.vidyardUserId
    where 
        h.tracker IN ('sharing_share_combo', 'video_creation_create_combo', 'insights_analytics_combo', 'manage_combo', 'admin_combo', 'product_sessions') 
    group by 1
)
select distinct
    h.userid as userid,
    last_player_created, 
    last_product_session, 
    last_event_time
from heap h
full outer join videos v 
    on v.userid = h.userid
    