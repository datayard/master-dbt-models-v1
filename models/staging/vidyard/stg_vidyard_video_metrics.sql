SELECT 
	vidyard_video_metrics.id as videoID ,
    vidyard_video_metrics.views_count as viewsCount ,
    vidyard_video_metrics.unique_viewers as uniqueViewers ,
    vidyard_video_metrics.identified_viewers as identifiedViewers ,
    vidyard_video_metrics.identified_views as identifiedViews
 FROM 
	{{ source('public', 'vidyard_video_metrics') }} as vidyard_video_metrics