SELECT 
        video_creation_started_to_create_or_upload_a_video_combo.event_id as eventID,
        video_creation_started_to_create_or_upload_a_video_combo.session_id as sessionID,
        video_creation_started_to_create_or_upload_a_video_combo.session_time as sessionTime,
        video_creation_started_to_create_or_upload_a_video_combo.user_id as userID,
        video_creation_started_to_create_or_upload_a_video_combo.time as eventTime,
        --video_creation_started_to_create_or_upload_a_video_combo.continent as continent,
        video_creation_started_to_create_or_upload_a_video_combo.country as country,
        video_creation_started_to_create_or_upload_a_video_combo.region as region,
        video_creation_started_to_create_or_upload_a_video_combo.city as city,
        video_creation_started_to_create_or_upload_a_video_combo.platform as platform,
        video_creation_started_to_create_or_upload_a_video_combo.device_type as deviceType,
        video_creation_started_to_create_or_upload_a_video_combo.browser as browser,
        --video_creation_started_to_create_or_upload_a_video_combo.browser_type as browserType,
        --video_creation_started_to_create_or_upload_a_video_combo.vidyard_platform as vidyardPlatform,
        video_creation_started_to_create_or_upload_a_video_combo.referrer as referrer,
        video_creation_started_to_create_or_upload_a_video_combo.landing_page as landingPage,
        video_creation_started_to_create_or_upload_a_video_combo.landing_page_query as landingPageQuery,
        video_creation_started_to_create_or_upload_a_video_combo.query as query,
        video_creation_started_to_create_or_upload_a_video_combo.domain as domain,
        video_creation_started_to_create_or_upload_a_video_combo.path as path,
        video_creation_started_to_create_or_upload_a_video_combo.title as title,
        video_creation_started_to_create_or_upload_a_video_combo.channels as channels
FROM
        {{ source ('govideo_production' , 'video_creation_started_to_create_or_upload_a_video_combo')}} as video_creation_started_to_create_or_upload_a_video_combo
WHERE
        video_creation_started_to_create_or_upload_a_video_combo.time < DATEADD(day, 1, current_date)