SELECT 
	vidyard_videos.id as videoID
	, vidyard_videos.user_id as userID
	, vidyard_videos.organization_id as organizationID
	, vidyard_videos.hosting_provider as hostingProvider
	, vidyard_videos.is_secure as isSecure
	, vidyard_videos.status as status
	, vidyard_videos.created_by_client_id as createdByClientID
	, case
			when vidyard_videos.created_by_client_id = 'auth.viewedit.com' or vidyard_videos.created_by_client_id = 'edge-extension.vidyard.com'
				then 'extension'
			when vidyard_videos.created_by_client_id = 'secure.vidyard.com' 
				then 'dashboard'
			when vidyard_videos.created_by_client_id = 'govideo-mobile.vidyard.com' or vidyard_videos.created_by_client_id = 'android.vidyard.com'
				then 'mobile'
			when vidyard_videos.created_by_client_id = 'desktop-mac.vidyard.com' or vidyard_videos.created_by_client_id = 'desktop-windows.vidyard.com'
			then 'Desktop'
			else 'partner-app'
		end as source
	, vidyard_videos.created_at as createdDate
	, vidyard_videos.origin as origin
	, case
    	when vidyard_videos.origin like '%chrome_extension%' then 'extension'
    	when vidyard_videos.origin like '%upload%' then 'upload'
    	when vidyard_videos.origin like '%blockbuster%' then 'partner_app'
    	when vidyard_videos.origin like '%mobile%' then 'mobile'
    	when vidyard_videos.origin like '%dashboard%' then 'dashboard'
		when vidyard_videos.origin like '%sample%' then 'borrowed or sample video'
    	--when vidyard_videos.origin is null then 'unknown'
		else 'unknown'
  	   end as derivedOrigin
	, vidyard_videos.milliseconds as milliseconds
	, vidyard_videos.updated_at as updatedDate
	, vidyard_videos.video_type as videoType
 FROM 
	{{ source('public', 'vidyard_videos') }} as vidyard_videos