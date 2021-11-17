SELECT
        viewed_watermark_landing_page.event_id as eventID,
        viewed_watermark_landing_page.session_id as sessionId,
        viewed_watermark_landing_page.session_time as sessionTime,
        viewed_watermark_landing_page.user_id as userID,
        viewed_watermark_landing_page.time as eventTime,
        viewed_watermark_landing_page.country as country,
        viewed_watermark_landing_page.region as region,
        viewed_watermark_landing_page.city as city,
        viewed_watermark_landing_page.platform as platform,
        viewed_watermark_landing_page.device as device,
        viewed_watermark_landing_page.device_type as deviceType,
        viewed_watermark_landing_page.browser as browser,
        viewed_watermark_landing_page.referrer as referrer,
        viewed_watermark_landing_page.landing_page as landingPage,
        viewed_watermark_landing_page.landing_page_query as landingPageQuery,
        viewed_watermark_landing_page.query as query,
        viewed_watermark_landing_page.domain as domain,
        viewed_watermark_landing_page.path as path,
        viewed_watermark_landing_page.title as title,
        viewed_watermark_landing_page.channels as channels
FROM
        {{ source ( 'govideo_production' , 'acquisition_viewed_watermark_landing_page')}} as viewed_watermark_landing_page
WHERE
        viewed_watermark_landing_page.time < DATEADD(day, 1, current_date)