SELECT 
	vidyard_videos.id as videoID,
	vidyard_videos.user_id as userID,
	vidyard_videos.organization_id as organizationID,
	vidyard_videos.hosting_provider as hostingProvider,
	vidyard_videos.is_secure as isSecure,
	vidyard_videos.status as status,
	vidyard_videos.created_by_client_id as createdByClientID,
	vidyard_videos.created_at as createdAt,
	vidyard_videos.origin as origin,
	vidyard_videos.milliseconds as milliseconds,
	vidyard_videos.updated_at as updatedAt,
	vidyard_videos.video_type as videoType
 FROM 
	{{ source('public', 'vidyard_videos') }} as vidyard_videos