SELECT
        viewed_backlink_landing_page.event_id as eventID,
        viewed_backlink_landing_page.session_id as sessionId,
        viewed_backlink_landing_page.session_time as sessionTime,
        viewed_backlink_landing_page.user_id as userID,
        viewed_backlink_landing_page.time as eventTime,
        viewed_backlink_landing_page.country as country,
        viewed_backlink_landing_page.region as region,
        viewed_backlink_landing_page.city as city,
        viewed_backlink_landing_page.platform as platform,
        viewed_backlink_landing_page.device_type as deviceType,
        viewed_backlink_landing_page.browser as browser,
        viewed_backlink_landing_page.referrer as referrer,
        viewed_backlink_landing_page.landing_page as landingPage,
        viewed_backlink_landing_page.landing_page_query as landingPageQuery,
        viewed_backlink_landing_page.query as query,
        viewed_backlink_landing_page.domain as domain,
        viewed_backlink_landing_page.path as path
FROM
        {{ source ( 'govideo_production' , 'viewed_backlink_landing_page') }} as viewed_backlink_landing_page
WHERE
        viewed_backlink_landing_page.time < DATEADD(day, 1, current_date)