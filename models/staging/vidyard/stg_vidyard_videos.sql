SELECT 
	vidyard_videos.id as videoID
	, vidyard_videos.user_id as userID
	, vidyard_videos.organization_id as organizationID
	, vidyard_videos.hosting_provider as hostingProvider
	, vidyard_videos.is_secure as isSecure
	, vidyard_videos.status as status
	, vidyard_videos.created_by_client_id as createdByClientID
	, vidyard_videos.created_at as createdAt
	, vidyard_videos.origin as origin
	, case
    	when vidyard_videos.origin like '%chrome_extension%' then 'extension'
    	when vidyard_videos.origin like '%upload%' then 'upload'
    	when vidyard_videos.origin like '%blockbuster%' then 'partner_app'
    	when vidyard_videos.origin like '%mobile%' then 'mobile'
    	when vidyard_videos.origin like '%dashboard%' then 'dashboard'
    	--when vidyard_videos.origin is null then 'unknown'
		else 'unknown'
  	   end as derived_origin
	, vidyard_videos.milliseconds as milliseconds
	, vidyard_videos.updated_at as updatedAt
	, vidyard_videos.video_type as videoType
 FROM 
	{{ source('public', 'vidyard_videos') }} as vidyard_videos