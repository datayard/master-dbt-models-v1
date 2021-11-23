SELECT
        signed_up_on_video_embed.event_id as eventID,
        signed_up_on_video_embed.session_id as sessionId,
        signed_up_on_video_embed.session_time as sessionTime,
        viewed_watermark_landing_page.user_id as userID,
        signed_up_on_video_embed.time as eventTime,
        signed_up_on_video_embed.country as country,
        signed_up_on_video_embed.region as region,
        signed_up_on_video_embed.city as city,
        signed_up_on_video_embed.platform as platform,
        signed_up_on_video_embed.device_type as deviceType,
        signed_up_on_video_embed.browser as browser,
        signed_up_on_video_embed.referrer as referrer,
        signed_up_on_video_embed.landing_page as landingPage,
        signed_up_on_video_embed.landing_page_query as landingPageQuery,
        signed_up_on_video_embed.query as query,
        signed_up_on_video_embed.domain as domain,
        signed_up_on_video_embed.path as path
FROM
        {{ source ( 'govideo_production' , 'sign_in_up_signed_up_on_video_embed_end_screen') }} as signed_up_on_video_embed
WHERE
        viewed_watermark_landing_page.time < DATEADD(day, 1, current_date)