select 
	hp_users.userid as heap_userid
	, hp_users."identity" as heap_vid_userid
    , hp_video_started.userid as video_started_heap_id
    , hp_video_finished.userid as video_finished_heap_id
from {{ ref('stg_govideo_production_users') }} hp_users
left join (select distinct userid from {{ ref('stg_govideo_production_video_creation_started_to_create_or_upload_a_video_combo') }}) hp_video_started
	on hp_video_started.userid = hp_users.userid
left join (select distinct userid from {{ ref('stg_govideo_production_video_recorded_or_uploaded') }}) hp_video_finished
	on hp_video_finished.userid = hp_users.userid
where hp_users."identity" is not null