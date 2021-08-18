SELECT 
	vidyard_org_metrics.id as organizationID
	, vidyard_org_metrics.total_seconds as totalSeconds
	, vidyard_org_metrics.views_count as viewsCount
	, vidyard_org_metrics.videos_with_views as videosWithViews
	, vidyard_org_metrics.first_view as firstView
	, TIMESTAMP 'epoch' + vidyard_org_metrics.first_view * INTERVAL '1 second' as firstViewDate
	, vidyard_org_metrics.first_view_video_id as firstViewVideoID
	, cast(case when vidyard_org_metrics.first_view is not null then 1 else 0 end as boolean) as activatedFlag
 FROM 
	{{ source('public', 'vidyard_org_metrics') }} as vidyard_org_metrics