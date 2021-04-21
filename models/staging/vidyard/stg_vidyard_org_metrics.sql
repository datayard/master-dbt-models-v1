SELECT 
	vidyard_org_metrics.id as organizationID,
	vidyard_org_metrics.total_seconds as totalSeconds,
	vidyard_org_metrics.views_count as viewsCount,
	vidyard_org_metrics.videos_with_views as videosWithViews,
	vidyard_org_metrics.first_view as firstView,
	vidyard_org_metrics.first_view_video_id as firstViewVideoID
 FROM 
	{{ source('public', 'vidyard_org_metrics') }} as vidyard_org_metrics