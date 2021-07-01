with vidyard_user_table as (
  select
      organizationid,
      createddate,
      userid,
      viewscount,
      count(distinct videoid) num_videos
  from {{ ref('vidyard_users_t2') }}
  where
      vidyard_email = 0
      and viewedit_email = 0
      and createdbyclientid != 'Enterprise'
      and orgtype = 'self_serve'
      and createddate >= '2020-01-01'::date
      and hubspot_personal_signup = 0
  group by 1, 2, 3, 4
)
, product_funnel_kube as (
  select
      vut.organizationid
      , case when hs.video_started_heap_id is not null then vut.organizationid end as heap_started_users
      , case when hs.video_finished_heap_id is not null then vut.organizationid end as heap_finished_users
      , case when vut.num_videos > 0 then vut.organizationid end as user_videos_not_deleted
  from
      vidyard_user_table vut
      left join {{ ref('heap_users_t2') }} hs 
          on vut.userid = cast(hs.heap_vid_userid as varchar(10))
)
select 
	'1-signups' as step
	, organizationid as users	--signups
	, organizationid as compare_with --signups
from product_funnel_kube
union                               
select 
	'2-started video creation' as step 
	, heap_started_users as users -- video creation started
	, organizationid as compare_with --signups
from product_funnel_kube 
union                               
select 
	'3-Video Created' as step 
	, heap_finished_users as users -- video creation completed
	, heap_started_users as compare_with --video creation started
from product_funnel_kube
union                               
select 
	'4-Has Video Not Deleted' as step
	, user_videos_not_deleted as users -- video not deleted
	, heap_finished_users as compare_with -- video creation completed
from product_funnel_kube                    
