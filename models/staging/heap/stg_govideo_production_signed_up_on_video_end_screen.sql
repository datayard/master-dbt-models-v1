SELECT
        signed_up_on_video_end.event_id as eventID,
        signed_up_on_video_end.session_id as sessionId,
        signed_up_on_video_end.session_time as sessionTime,
        signed_up_on_video_end.user_id as userID,
        signed_up_on_video_end.time as eventTime,
        signed_up_on_video_end.country as country,
        signed_up_on_video_end.region as region,
        signed_up_on_video_end.city as city,
        signed_up_on_video_end.platform as platform,
        signed_up_on_video_end.device_type as deviceType,
        signed_up_on_video_end.browser as browser,
        signed_up_on_video_end.referrer as referrer,
        signed_up_on_video_end.landing_page as landingPage,
        signed_up_on_video_end.landing_page_query as landingPageQuery,
        signed_up_on_video_end.query as query,
        signed_up_on_video_end.domain as domain,
        signed_up_on_video_end.path as path
FROM
        {{ source ( 'govideo_production' , 'sign_in_up_signed_up_on_video_embed_end_screen') }} as signed_up_on_video_end
WHERE
        signed_up_on_video_end.time < DATEADD(day, 1, current_date)