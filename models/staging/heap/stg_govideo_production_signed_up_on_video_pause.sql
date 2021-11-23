SELECT
        signed_up_on_video_pause.event_id as eventID,
        signed_up_on_video_pause.session_id as sessionId,
        signed_up_on_video_pause.session_time as sessionTime,
        signed_up_on_video_pause.user_id as userID,
        signed_up_on_video_pause.time as eventTime,
        signed_up_on_video_pause.country as country,
        signed_up_on_video_pause.region as region,
        signed_up_on_video_pause.city as city,
        signed_up_on_video_pause.platform as platform,
        signed_up_on_video_pause.device_type as deviceType,
        signed_up_on_video_pause.browser as browser,
        signed_up_on_video_pause.referrer as referrer,
        signed_up_on_video_pause.landing_page as landingPage,
        signed_up_on_video_pause.landing_page_query as landingPageQuery,
        signed_up_on_video_pause.query as query,
        signed_up_on_video_pause.domain as domain,
        signed_up_on_video_pause.path as path
FROM
        {{ source ( 'govideo_production' , 'sign_in_up_signed_up_on_video_pause') }} as signed_up_on_video_pause
WHERE
        signed_up_on_video_pause.time < DATEADD(day, 1, current_date)