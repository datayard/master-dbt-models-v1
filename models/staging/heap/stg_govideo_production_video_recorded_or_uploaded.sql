SELECT
        video_recorded_or_uploaded.event_id as eventID,
        video_recorded_or_uploaded.session_id as sessionId,
        video_recorded_or_uploaded.session_time as sessionTime,
        video_recorded_or_uploaded.user_id as userID,
        video_recorded_or_uploaded.time as eventTime,
        --video_recorded_or_uploaded.continent as continent,
        video_recorded_or_uploaded.country as country,
        video_recorded_or_uploaded.region as region,
        video_recorded_or_uploaded.city as city,
        video_recorded_or_uploaded.platform as platform,
        video_recorded_or_uploaded.device as device,
        video_recorded_or_uploaded.device_type as deviceType,
        video_recorded_or_uploaded.browser as browser,
        --video_recorded_or_uploaded.browser_type as browserType,
        --video_recorded_or_uploaded.vidyard_platform as vidyardPlatform,
        video_recorded_or_uploaded.referrer as referrer,
        video_recorded_or_uploaded.landing_page as landingPage,
        video_recorded_or_uploaded.landing_page_query as landingPageQuery,
        video_recorded_or_uploaded.query as query,
        video_recorded_or_uploaded.domain as domain,
        video_recorded_or_uploaded.path as path,
        video_recorded_or_uploaded.title as title,
        video_recorded_or_uploaded.channels as channels
FROM
        {{ source ( 'govideo_production' , 'video_recorded_or_uploaded')}} as video_recorded_or_uploaded
WHERE
        TRUE